import awswrangler as wr
import boto3
import pytest
import yaml
import logging

from botocore.exceptions import NoCredentialsError
from datetime import date
from floorist.floorist import main, _is_retryable_db_error, _safe_rollback, _cleanup_s3_target, _dump_with_retry, MAX_RETRIES, RETRY_DELAY
from os import environ as env
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy.exc import OperationalError
from tempfile import NamedTemporaryFile
from unittest.mock import Mock, patch, MagicMock


class TestFloorist:
    @pytest.fixture(autouse=True)
    def setup_env(self):
        with open('tests/env.yaml', 'r') as stream:
            settings = yaml.safe_load(stream)
            for key in settings:
                env[key] = settings[key]

    @pytest.fixture(autouse=True)
    def setup_caplog(self, caplog):
        caplog.set_level(logging.INFO)

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
        assert 'Dumped 2 from total of 2' in caplog.text
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/numbers/", f"{prefix}/people/"]

    def test_floorplan_with_large_result(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_large_result.yaml'
        main()
        assert 'Dumped 1 from total of 1' in caplog.text
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/series/"]
        assert len(wr.s3.list_objects(f"{prefix}/series/", boto3_session=session)) == 1000
        df = wr.s3.read_parquet(f"{prefix}/series/", boto3_session=session)
        assert len(df), 1000000

    def test_floorplan_with_custom_chunksize(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_custom_chunksize.yaml'
        main()
        assert 'Dumped 1 from total of 1' in caplog.text
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/series/"]
        assert len(wr.s3.list_objects(f"{prefix}/series/", boto3_session=session)) == 77
        df = wr.s3.read_parquet(f"{prefix}/series/", boto3_session=session)
        assert len(df), 1000

    def test_floorplan_with_zero_chunksize(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_zero_chunksize.yaml'
        main()
        assert 'Dumped 1 from total of 1' in caplog.text
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/series/"]
        assert len(wr.s3.list_objects(f"{prefix}/series/", boto3_session=session)) == 1
        df = wr.s3.read_parquet(f"{prefix}/series/", boto3_session=session)
        assert len(df), 1000

    def test_floorplan_with_one_failing_dump(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_one_failing_dump.yaml'
        with pytest.raises(SystemExit) as ex:
            main()
        assert ex.value.code == 1
        assert 'ProgrammingError' in caplog.text
        assert 'Dumped 1 from total of 2' in caplog.text
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/numbers/"]

    def test_floorplan_with_empty_dataset(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        datepath = f"{date.today().strftime('year_created=%Y/month_created=%-m/day_created=%-d')}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_with_empty_dataset.yaml'
        main()
        assert 'Dumped 1 from total of 1' in caplog.text
        assert wr.s3.list_directories(f"{prefix}/empty/{datepath}", boto3_session=session) == [f"{prefix}/empty/{datepath}/"]
        # Deep directories need a different cleanup approach
        wr._utils.resource('s3').Bucket(env['AWS_BUCKET']).objects.all().delete()

    def test_floorplan_valid(self, caplog, session):
        prefix = f"s3://{env['AWS_BUCKET']}"
        env['FLOORPLAN_FILE'] = 'tests/floorplan_valid.yaml'
        main()
        assert 'Dumped 1 from total of 1' in caplog.text
        assert wr.s3.list_directories(prefix, boto3_session=session) == [f"{prefix}/valid/"]
        assert len(wr.s3.list_objects(f"{prefix}/valid/", boto3_session=session)) == 1
        df = wr.s3.read_parquet(f"{prefix}/valid/", boto3_session=session)
        assert len(df), 3


@pytest.mark.standalone
class TestIsRetryableDbError:
    """Test suite for _is_retryable_db_error function."""

    def test_serialization_failure_is_retryable(self):
        """Test that SerializationFailure errors are identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="SerializationFailure: terminating connection")

        result = _is_retryable_db_error(mock_ex)

        assert result is True, "SerializationFailure should be retryable"

    def test_conflict_with_recovery_is_retryable(self):
        """Test that 'conflict with recovery' errors are identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="terminating connection due to conflict with recovery")

        result = _is_retryable_db_error(mock_ex)

        assert result is True, "'conflict with recovery' should be retryable"

    def test_pending_rollback_error_is_retryable(self):
        """Test that PendingRollbackError is identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.PendingRollbackError)
        mock_ex.__str__ = Mock(return_value="PendingRollbackError: invalid transaction")

        result = _is_retryable_db_error(mock_ex)

        assert result is True, "PendingRollbackError should be retryable"

    def test_invalid_transaction_is_retryable(self):
        """Test that 'invalid transaction' errors are identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="Can't reconnect until invalid transaction is rolled back")

        result = _is_retryable_db_error(mock_ex)

        assert result is True, "'invalid transaction' should be retryable"

    def test_non_retryable_error(self):
        """Test that non-retryable errors are identified correctly."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="Some other database error")

        result = _is_retryable_db_error(mock_ex)

        assert result is False, "Non-retryable errors should return False"

    def test_connection_error_is_not_retryable(self):
        """Test that connection errors are not identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="Connection refused")

        result = _is_retryable_db_error(mock_ex)

        assert result is False, "Connection errors should not be retryable"


@pytest.mark.standalone
class TestSafeRollback:
    """Test suite for _safe_rollback function."""

    @pytest.fixture
    def mock_conn(self):
        """Mock database connection."""
        return Mock()

    @pytest.fixture
    def dump_count(self):
        """Dump count for testing."""
        return 42

    @patch('floorist.floorist.logging')
    def test_successful_rollback_logs_warning(self, mock_logging, mock_conn, dump_count):
        """Test that successful rollback logs appropriate warning."""
        _safe_rollback(mock_conn, dump_count)

        mock_conn.rollback.assert_called_once()
        mock_logging.warning.assert_called_once_with(
            "[Dump #%d] Database serialization/recovery conflict detected. "
            "Rolling back transaction.",
            dump_count
        )

    @patch('floorist.floorist.logging')
    def test_rollback_failure_logs_error(self, mock_logging, mock_conn, dump_count):
        """Test that rollback failure logs appropriate warning."""
        rollback_error = Exception("Rollback failed")
        mock_conn.rollback.side_effect = rollback_error

        _safe_rollback(mock_conn, dump_count)

        mock_conn.rollback.assert_called_once()
        # Should only log the rollback error, not the success message
        mock_logging.error.assert_called_once_with(
            "[Dump #%d] Error during rollback: %s", dump_count, rollback_error
        )


@pytest.mark.standalone
class TestCleanupS3Target:
    """Test suite for _cleanup_s3_target function and S3 cleanup failure handling."""

    @pytest.fixture
    def dump_count(self):
        """Dump count for testing."""
        return 42

    @pytest.fixture
    def target(self):
        """S3 target path."""
        return "s3://test-bucket/test-prefix/year_created=2025/month_created=1/day_created=1"

    @patch('floorist.floorist.logging')
    @patch('floorist.floorist.wr.s3.delete_objects')
    def test_cleanup_s3_target_success(self, mock_delete, mock_logging, dump_count, target):
        """Test that _cleanup_s3_target returns True on successful deletion."""
        mock_delete.return_value = None

        result = _cleanup_s3_target(dump_count, target)

        assert result is True, "Should return True on successful cleanup"
        mock_delete.assert_called_once_with(target)
        mock_logging.info.assert_called_once_with(
            '[Dump #%d] Cleaning up S3 target before retry: %s',
            dump_count,
            target
        )
        mock_logging.error.assert_not_called()

    @patch('floorist.floorist.logging')
    @patch('floorist.floorist.wr.s3.delete_objects')
    def test_cleanup_s3_target_failure(self, mock_delete, mock_logging, dump_count, target):
        """Test that _cleanup_s3_target returns False on deletion failure."""
        cleanup_error = Exception("Access Denied")
        mock_delete.side_effect = cleanup_error

        result = _cleanup_s3_target(dump_count, target)

        assert result is False, "Should return False on cleanup failure"
        mock_delete.assert_called_once_with(target)
        mock_logging.error.assert_called_once_with(
            '[Dump #%d] Failed to cleanup S3 target before retry: %s',
            dump_count,
            cleanup_error
        )

    @patch('floorist.floorist.logging')
    @patch('floorist.floorist.wr.s3.delete_objects')
    def test_cleanup_s3_target_handles_various_exceptions(self, mock_delete, mock_logging, dump_count, target):
        """Test that _cleanup_s3_target handles different exception types."""
        # Test with different exception types
        exceptions = [
            Exception("Generic error"),
            IOError("IO error"),
            RuntimeError("Runtime error"),
        ]

        for exc in exceptions:
            mock_delete.side_effect = exc
            mock_logging.reset_mock()

            result = _cleanup_s3_target(dump_count, target)

            assert result is False, f"Should return False for {type(exc).__name__}"
            mock_logging.error.assert_called_once()

    @patch('floorist.floorist.logging')
    @patch('floorist.floorist._cleanup_s3_target')
    @patch('pandas.read_sql')
    def test_retry_fails_when_s3_cleanup_fails(self, mock_read_sql, mock_cleanup, mock_logging):
        """Test that retry attempt fails immediately when S3 cleanup fails."""
        mock_cleanup.return_value = False

        mock_conn = Mock()
        mock_config = Mock(bucket_name="test-bucket")

        mock_read_sql.side_effect = [
            sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: terminating connection"),
                connection_invalidated=False
            ),
            MagicMock()
        ]

        row = {'query': 'SELECT 1', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_conn, mock_config, dump_count=1)

        assert result is False, "Dump should fail when S3 cleanup fails"
        mock_cleanup.assert_called()

        mock_logging.error.assert_any_call(
            '[Dump #%d] Cannot retry due to S3 cleanup failure', 1
        )

        assert mock_read_sql.call_count == 1, "Should not retry after S3 cleanup fails"


@pytest.mark.standalone
class TestRetryIntegration:
    """Integration tests for retry behavior in _dump_with_retry()."""

    @patch('floorist.floorist.wr.s3.to_parquet')
    @patch('floorist.floorist.wr.s3.delete_objects')
    @patch('floorist.floorist.logging')
    @patch('pandas.read_sql')
    def test_single_retry_succeeds(self, mock_read_sql, mock_logging, mock_s3_delete, mock_s3_write):
        """Test that a single retry is successful after SerializationFailure."""
        import pandas as pd

        mock_conn = Mock()
        mock_config = Mock(bucket_name="test-bucket")

        test_df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})

        mock_read_sql.side_effect = [
            sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: terminating connection"),
                connection_invalidated=False
            ),
            [test_df]
        ]

        row = {'query': 'SELECT * FROM test', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_conn, mock_config, dump_count=1)

        assert result is True, "Dump should succeed on retry"
        assert mock_read_sql.call_count == 2, "Should call read_sql twice (initial + 1 retry)"
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_s3_delete.assert_called_once()
        mock_s3_write.assert_called_once()

        mock_logging.info.assert_any_call(
            '[Dump #%d] Retry %d of %d (attempt %d total)', 1, 1, MAX_RETRIES - 1, 2
        )

    @patch('floorist.floorist.wr.s3.to_parquet')
    @patch('floorist.floorist.wr.s3.delete_objects')
    @patch('floorist.floorist.logging')
    @patch('pandas.read_sql')
    def test_failure_mid_chunk_processing_retries_successfully(self, mock_read_sql, mock_logging, mock_s3_delete, mock_s3_write):
        """Test that failure during chunk processing triggers retry and succeeds."""
        import pandas as pd

        mock_conn = Mock()
        mock_config = Mock(bucket_name="test-bucket")

        chunk1 = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        chunk2 = pd.DataFrame({'id': [3, 4], 'value': ['c', 'd']})
        chunk3 = pd.DataFrame({'id': [5, 6], 'value': ['e', 'f']})

        def failing_iterator():
            yield chunk1
            yield chunk2
            raise sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: conflict with recovery"),
                connection_invalidated=False
            )

        mock_read_sql.side_effect = [
            failing_iterator(),
            [chunk1, chunk2, chunk3]
        ]

        row = {'query': 'SELECT * FROM test', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_conn, mock_config, dump_count=1)

        assert result is True, "Dump should succeed on retry"
        assert mock_read_sql.call_count == 2, "Should call read_sql twice (initial + 1 retry)"
        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_s3_delete.assert_called_once()

        assert mock_s3_write.call_count == 5, "Should write 2 chunks (failed attempt) + 3 chunks (successful retry)"

        # Check that chunk writes were logged (first attempt: chunks 1, 2; retry: chunks 1, 2, 3)
        chunk_log_calls = [
            c for c in mock_logging.info.call_args_list
            if len(c[0]) >= 2 and c[0][0] == '[Dump #%d] Written parquet chunk #%d'
        ]
        assert len(chunk_log_calls) == 5, "Should log all chunk writes (2 + 3)"

    @patch('floorist.floorist.wr.s3.to_parquet')
    @patch('floorist.floorist.wr.s3.delete_objects')
    @patch('floorist.floorist.logging')
    @patch('pandas.read_sql')
    @patch('floorist.floorist.time.sleep')
    def test_exhausted_retries_fails(self, mock_sleep, mock_read_sql, mock_logging, mock_s3_delete, mock_s3_write):
        """Test that exhausting all retries results in failure."""
        mock_conn = Mock()
        mock_config = Mock(bucket_name="test-bucket")

        mock_read_sql.side_effect = sqlalchemy_exc.OperationalError(
            "statement", "params",
            orig=Exception("SerializationFailure: terminating connection"),
            connection_invalidated=False
        )

        row = {'query': 'SELECT * FROM test', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_conn, mock_config, dump_count=1)

        assert result is False, "Dump should fail after exhausting retries"

        # Assert exponential backoff behavior on time.sleep
        expected_delays = [RETRY_DELAY * (2 ** i) for i in range(MAX_RETRIES - 1)]
        actual_delays = [c.args[0] for c in mock_sleep.call_args_list]
        assert actual_delays == expected_delays

        assert mock_read_sql.call_count == MAX_RETRIES, f"Should call read_sql {MAX_RETRIES} times (initial + {MAX_RETRIES - 1} retries)"
        assert mock_conn.rollback.call_count == MAX_RETRIES, f"Should rollback {MAX_RETRIES} times"
        mock_conn.commit.assert_not_called()
        assert mock_s3_delete.call_count == MAX_RETRIES - 1, "Should cleanup S3 before each retry"
        mock_s3_write.assert_not_called()

    @patch('floorist.floorist.wr.s3.to_parquet')
    @patch('floorist.floorist.wr.s3.delete_objects')
    @patch('floorist.floorist.logging')
    @patch('pandas.read_sql')
    def test_multiple_dumps_with_one_retry(self, mock_read_sql, mock_logging, mock_s3_delete, mock_s3_write):
        """Test that retry logic works correctly when called multiple times for different dumps."""
        import pandas as pd

        mock_conn = Mock()
        mock_config = Mock(bucket_name="test-bucket")

        df1 = pd.DataFrame({'id': [1, 2]})
        df2 = pd.DataFrame({'id': [3, 4]})

        # First dump succeeds immediately
        # Second dump fails once, then succeeds
        mock_read_sql.side_effect = [
            [df1],
            sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: terminating connection"),
                connection_invalidated=False
            ),
            [df2]
        ]

        row1 = {'query': 'SELECT * FROM table1', 'prefix': 'prefix1', 'chunksize': 1000}
        row2 = {'query': 'SELECT * FROM table2', 'prefix': 'prefix2', 'chunksize': 1000}

        result1 = _dump_with_retry(row1, mock_conn, mock_config, dump_count=1)
        result2 = _dump_with_retry(row2, mock_conn, mock_config, dump_count=2)

        assert result1 is True, "First dump should succeed"
        assert result2 is True, "Second dump should succeed on retry"
        assert mock_read_sql.call_count == 3, "Should call read_sql 3 times (dump1, dump2 fail, dump2 retry)"
        assert mock_conn.rollback.call_count == 1, "Should rollback once for dump2"
        assert mock_conn.commit.call_count == 2, "Should commit twice (dump1 success, dump2 retry success)"
        assert mock_s3_delete.call_count == 1, "Should cleanup S3 once before dump2 retry"
        assert mock_s3_write.call_count == 2, "Should write twice (dump1, dump2 retry)"
