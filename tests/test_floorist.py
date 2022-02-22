import pytest
import yaml

from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from os import environ as env
from psycopg2 import OperationalError
from s3fs import S3FileSystem
from yaml.parser import ParserError
from floorist.floorist import main
from tempfile import NamedTemporaryFile


class TestFloorist:
    @pytest.fixture(autouse=True)
    def setup_env(self):
        with open('tests/env.yaml', 'r') as stream:
            settings = yaml.safe_load(stream)
            for key in settings:
                env[key] = settings[key]

    @pytest.mark.parametrize('key', ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_REGION'])
    def test_unset_s3_credentials(self, key):
        # FIXME: botocore caches the environment variables in a weird way, so their deletion
        # leaks into other tests and affects them, even if the variables are reset after each run
        return

        del env[key]
        with pytest.raises(NoCredentialsError):
            main()

    def test_invalid_s3_credentials(self):
        del env['AWS_ACCESS_KEY_ID']
        with pytest.raises(PartialCredentialsError) as ex:
            main()

    def test_unset_s3_bucket(self):
        del env['AWS_BUCKET']
        with pytest.raises(ValueError, match=r".*Bucket name not configured.*"):
            main()

    def test_missing_s3_bucket(self):
        env['AWS_BUCKET'] = 'foo'
        with pytest.raises(FileNotFoundError) as ex:
            main()
        assert 'bucket does not exist' in str(ex.value)

    @pytest.mark.parametrize('key',
                             ['POSTGRES_SERVICE_HOST', 'POSTGRESQL_USER', 'POSTGRESQL_DATABASE', 'POSTGRESQL_PASSWORD'])
    def test_missing_pg_credentials(self, key):
        del env[key]
        with pytest.raises(ValueError, match=".*not defined"):
            main()

    def test_invalid_pg_credentials(self):
        env['POSTGRESQL_USER'] = 'foo'
        with pytest.raises(OperationalError) as ex:
            main()

    def test_invalid_pg_databae(self):
        env['POSTGRESQL_DATABASE'] = 'foo'
        with pytest.raises(OperationalError) as ex:
            main()
        assert 'database "foo" does not exist' in str(ex.value)

    def test_unset_floorplan(self):
        del env['FLOORPLAN_FILE']
        with pytest.raises(ValueError, match="Floorplan filename not defined"):
            main()

    def test_missing_floorplan(self):
        env['FLOORPLAN_FILE'] = 'foo'
        with pytest.raises(IOError):
            main()

    def test_floorplan_undefined_aws_endpoint(self, caplog):
        del env['AWS_ENDPOINT']
        with pytest.raises(ValueError, match="endpoint not defined"):
            main()

    @pytest.mark.skip(reason="broken by issue #2")
    def test_empty_floorplan(self):
        with pytest.raises(ParserError):
            with NamedTemporaryFile(mode='w+t') as tempfile:
                env['FLOORPLAN_FILE'] = tempfile.name
                main()

    @pytest.mark.skip(reason="broken by issue #3")
    def test_invalid_floorplan(self):
        with pytest.raises(ParserError):
            with NamedTemporaryFile(mode='w+t') as tempfile:
                tempfile.write('Some invalid floorplan')
                tempfile.flush()
                env['FLOORPLAN_FILE'] = tempfile.name
                main()

    def test_floorplan_without_query(self, caplog):
        env['FLOORPLAN_FILE'] = 'tests/floorplan_without_query.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'query' in caplog.text
        assert 'KeyError' in caplog.text

    def test_floorplan_without_prefix(self, caplog):
        env['FLOORPLAN_FILE'] = 'tests/floorplan_without_prefix.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'KeyError' in caplog.text
        assert 'prefix' in caplog.text

    def test_floorplan_with_invalid_query(self, caplog):
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_invalid_query.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'DatabaseError' in caplog.text

    def test_floorplan_with_invalid_prefix(self, caplog):
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_invalid_prefix.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'XMinioInvalidObjectName' in caplog.text

    def test_floorplan_with_multiple_dumps(self, caplog):
        s3 = S3FileSystem(client_kwargs={'endpoint_url': env.get('AWS_ENDPOINT')})
        if s3.ls(env['AWS_BUCKET']) != []:  # Make sure that the bucket is empty
            s3.rm(f"{env['AWS_BUCKET']}/*", recursive=True)

        assert s3.ls(env['AWS_BUCKET']) == []
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_multiple_dumps.yaml'
        main()
        assert 'Dumped 2 from total of 2'
        assert s3.ls(env['AWS_BUCKET']) == [f"{env['AWS_BUCKET']}/numbers", f"{env['AWS_BUCKET']}/people"]

    def test_floorplan_with_one_failing_dump(self, caplog):
        s3 = S3FileSystem(client_kwargs={'endpoint_url': env.get('AWS_ENDPOINT')})
        if s3.ls(env['AWS_BUCKET']) != []:  # Make sure that the bucket is empty
            s3.rm(f"{env['AWS_BUCKET']}/*", recursive=True)

        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_one_failing_dump.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'DatabaseError' in caplog.text
        assert 'Dumped 1 from total of 2'
        assert s3.ls('floorist') == [f"{env['AWS_BUCKET']}/numbers"]
