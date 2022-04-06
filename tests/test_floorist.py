import awswrangler as wr
import boto3
import pytest
import yaml

from botocore.exceptions import NoCredentialsError
from floorist.floorist import main
from os import environ as env
from sqlalchemy.exc import OperationalError
from tempfile import NamedTemporaryFile


class TestFloorist:
    @pytest.fixture(autouse=True)
    def setup_env(self):
        with open('tests/env.yaml', 'r') as stream:
            settings = yaml.safe_load(stream)
            for key in settings:
                env[key] = settings[key]

    @pytest.fixture(autouse=False)
    def session(self):
        prefix = f"s3://{env['AWS_BUCKET']}"
        # Setup the boto3 session
        wr.config.s3_endpoint_url = env['AWS_ENDPOINT']
        session = boto3.Session(
            aws_access_key_id=env['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=env['AWS_SECRET_ACCESS_KEY'],
            region_name=env['AWS_REGION']
        )

        # Make sure that the bucket is empty
        if wr.s3.list_objects(prefix, boto3_session=session) != []:
            wr.s3.delete_objects(f"s3://{env['AWS_BUCKET']}/*", boto3_session=session)
        assert wr.s3.list_objects(prefix, boto3_session=session) == []

        return session

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
        with pytest.raises(Exception) as ex:
            main()

    def test_unset_s3_bucket(self):
        del env['AWS_BUCKET']
        with pytest.raises(ValueError, match=r".*Bucket name not configured.*"):
            main()

    def test_missing_s3_bucket(self):
        env['AWS_BUCKET'] = 'foo'
        with pytest.raises(Exception) as ex:
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
        with pytest.raises(yaml.parser.ParserError):
            with NamedTemporaryFile(mode='w+t') as tempfile:
                env['FLOORPLAN_FILE'] = tempfile.name
                main()

    @pytest.mark.skip(reason="broken by issue #3")
    def test_invalid_floorplan(self):
        with pytest.raises(yaml.parser.ParserError):
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
        assert 'syntax error' in caplog.text

    def test_floorplan_with_invalid_prefix(self, caplog):
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_invalid_prefix.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'XMinioInvalidObjectName' in caplog.text

    def test_floorplan_with_multiple_dumps(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_multiple_dumps.yaml'
        main()
        assert 'Dumped 2 from total of 2'
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/numbers/", f"{prefix}/people/"]

    def test_floorplan_with_large_result(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_large_result.yaml'
        main()
        assert 'Dumped 1 from total of 1'
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/series/"]
        assert len(wr.s3.list_objects(f"{prefix}/series/", boto3_session=session)) == 1000
        df = wr.s3.read_parquet(f"{prefix}/series/", boto3_session=session)
        assert len(df), 1000000

    def test_floorplan_with_custom_chunksize(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_custom_chunksize.yaml'
        main()
        assert 'Dumped 1 from total of 1'
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/series/"]
        assert len(wr.s3.list_objects(f"{prefix}/series/", boto3_session=session)) == 77
        df = wr.s3.read_parquet(f"{prefix}/series/", boto3_session=session)
        assert len(df), 1000

    def test_floorplan_with_one_failing_dump(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_one_failing_dump.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'ProgrammingError' in caplog.text
        assert 'Dumped 1 from total of 2'
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/numbers/"]
