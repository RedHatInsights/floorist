import botocore.exceptions
import pandas as pd
import pytest
import yaml

from floorist.floorist import main, RetryPolicy, RetryResult, _dump_with_retry, MAX_RETRIES, RETRY_DELAY
from os import environ
from sqlalchemy import exc as sqlalchemy_exc
from unittest.mock import Mock, patch, MagicMock, mock_open


@pytest.mark.standalone
class TestIsRetryableDbError:
    """Test suite for _is_retryable_db_error function."""

    def test_serialization_failure_is_retryable(self):
        """Test that SerializationFailure errors are identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="SerializationFailure: terminating connection")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=1)
        assert result == RetryResult.RETRY, "SerializationFailure should be retryable"

    def test_conflict_with_recovery_is_retryable(self):
        """Test that 'conflict with recovery' errors are identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="terminating connection due to conflict with recovery")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=1)
        assert result == RetryResult.RETRY, "'conflict with recovery' should be retryable"

    def test_pending_rollback_error_is_retryable(self):
        """Test that PendingRollbackError is identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.PendingRollbackError)
        mock_ex.__str__ = Mock(return_value="PendingRollbackError: invalid transaction")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=1)
        assert result == RetryResult.RETRY, "PendingRollbackError should be retryable"

    def test_invalid_transaction_is_retryable(self):
        """Test that 'invalid transaction' errors are identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="Can't reconnect until invalid transaction is rolled back")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=1)
        assert result == RetryResult.RETRY, "'invalid transaction' should be retryable"

    def test_non_retryable_error(self):
        """Test that non-retryable errors are identified correctly."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="Some other database error")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=1)
        assert result == RetryResult.FAILURE, "Non-retryable errors should return False"

    def test_connection_error_is_not_retryable(self):
        """Test that connection errors are not identified as retryable."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="Connection refused")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=1)
        assert result == RetryResult.FAILURE, "Connection errors should not be retryable"

    def test_retryable_error_first_attempt_returns_retry(self):
        """Test that the first failure attempt retries."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="SerializationFailure: terminating connection")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=0)
        assert result == RetryResult.RETRY

    def test_retryable_error_last_attempt_returns_exhausted(self):
        """Test that the last failure attempt changes the state to EXHAUSTED."""
        mock_ex = Mock(spec=sqlalchemy_exc.OperationalError)
        mock_ex.__str__ = Mock(return_value="SerializationFailure: terminating connection")
        result = RetryPolicy(max_retries=3).evaluate(mock_ex, attempt=3)
        assert result == RetryResult.EXHAUSTED


@pytest.mark.standalone
class TestS3CleanupFailure:
    """Test that S3 cleanup failure during retry prevents further attempts."""

    @pytest.fixture
    def mock_s3(self):
        mock = Mock()
        mock.make_path.return_value = (
            "test-prefix/year_created=2025/month_created=1/day_created=1",
            "s3://test-bucket/test-prefix/year_created=2025/month_created=1/day_created=1",
        )
        return mock

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @patch('floorist.floorist.logging')
    def test_retry_fails_when_s3_cleanup_fails(self, mock_logging, mock_db, mock_s3):
        """Test that retry attempt fails immediately when S3 cleanup fails."""
        mock_s3.cleanup.side_effect = Exception("Access Denied")

        mock_db.execute_query.side_effect = [
            sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: terminating connection"),
                connection_invalidated=False
            ),
            iter([MagicMock()])
        ]

        row = {'query': 'SELECT 1', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_db, mock_s3, dump_count=1)

        assert result is False, "Dump should fail when S3 cleanup fails"
        mock_s3.cleanup.assert_called_once()
        mock_logging.exception.assert_any_call(
            '[Dump #%d] S3 cleanup failed, cannot retry', 1
        )
        assert mock_db.execute_query.call_count == 1, "Should not retry after S3 cleanup fails"


@pytest.mark.standalone
class TestRetryIntegration:
    """Integration tests for retry behavior in _dump_with_retry()."""

    @pytest.fixture
    def mock_s3(self):
        mock = Mock()
        mock.make_path.return_value = (
            "test-prefix/year_created=2025/month_created=1/day_created=1",
            "s3://test-bucket/test-prefix/year_created=2025/month_created=1/day_created=1",
        )
        return mock

    @pytest.fixture
    def mock_db(self):
        return Mock()

    @patch('floorist.floorist.logging')
    def test_single_retry_succeeds(self, mock_logging, mock_s3, mock_db):
        """Test that a single retry is successful after SerializationFailure."""
        test_df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})

        mock_db.execute_query.side_effect = [
            sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: terminating connection"),
                connection_invalidated=False
            ),
            iter([test_df])
        ]

        row = {'query': 'SELECT * FROM test', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_db, mock_s3, dump_count=1)

        assert result is True, "Dump should succeed on retry"
        assert mock_db.execute_query.call_count == 2, \
            "Should call execute_query twice (initial + 1 retry)"
        mock_db.rollback.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_s3.cleanup.assert_called_once()
        mock_s3.write_parquet.assert_called_once()

        mock_logging.info.assert_any_call(
            '[Dump #%d] Retry %d of %d (attempt %d total)', 1, 1, MAX_RETRIES - 1, 2
        )

    @patch('floorist.floorist.logging')
    def test_failure_mid_chunk_processing_retries_successfully(self, mock_logging, mock_s3, mock_db):
        """Test that failure during chunk processing triggers retry and succeeds."""
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

        mock_db.execute_query.side_effect = [
            failing_iterator(),
            iter([chunk1, chunk2, chunk3])
        ]

        row = {'query': 'SELECT * FROM test', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_db, mock_s3, dump_count=1)

        assert result is True, "Dump should succeed on retry"
        assert mock_db.execute_query.call_count == 2, "Should call execute_query twice (initial + 1 retry)"
        mock_db.rollback.assert_called_once()
        mock_db.commit.assert_called_once()
        mock_s3.cleanup.assert_called_once()

        assert mock_s3.write_parquet.call_count == 5, \
            "Should write 2 chunks (failed attempt) + 3 chunks (successful retry)"

        # Check that chunk writes were logged (first attempt: chunks 1, 2; retry: chunks 1, 2, 3)
        chunk_log_calls = [
            c for c in mock_logging.info.call_args_list
            if len(c[0]) >= 2 and c[0][0] == '[Dump #%d] Written parquet chunk #%d'
        ]
        assert len(chunk_log_calls) == 5, "Should log all chunk writes (2 + 3)"

    @patch('floorist.floorist.logging')
    @patch('floorist.floorist.time.sleep')
    def test_exhausted_retries_fails(self, mock_sleep, mock_logging, mock_s3, mock_db):
        """Test that exhausting all retries results in failure."""
        mock_db.execute_query.side_effect = sqlalchemy_exc.OperationalError(
            "statement", "params",
            orig=Exception("SerializationFailure: terminating connection"),
            connection_invalidated=False
        )

        row = {'query': 'SELECT * FROM test', 'prefix': 'test-prefix', 'chunksize': 1000}
        result = _dump_with_retry(row, mock_db, mock_s3, dump_count=1)

        assert result is False, "Dump should fail after exhausting retries"

        # Assert exponential backoff behavior on time.sleep
        expected_delays = [RETRY_DELAY * (2 ** i) for i in range(MAX_RETRIES - 1)]
        actual_delays = [c.args[0] for c in mock_sleep.call_args_list]
        assert actual_delays == expected_delays

        assert mock_db.execute_query.call_count == MAX_RETRIES, \
            f"Should call execute_query {MAX_RETRIES} times (initial + {MAX_RETRIES - 1} retries)"
        assert mock_db.rollback.call_count == MAX_RETRIES, f"Should rollback {MAX_RETRIES} times"
        mock_db.commit.assert_not_called()
        assert mock_s3.cleanup.call_count == MAX_RETRIES - 1, \
            "Should cleanup S3 before each retry"
        mock_s3.write_parquet.assert_not_called()

    @patch('floorist.floorist.logging')
    def test_multiple_dumps_with_one_retry(self, mock_logging, mock_s3, mock_db):
        """Test that retry logic works correctly when called multiple times for different dumps."""
        df1 = pd.DataFrame({'id': [1, 2]})
        df2 = pd.DataFrame({'id': [3, 4]})

        # First dump succeeds immediately
        # Second dump fails once, then succeeds
        mock_db.execute_query.side_effect = [
            iter([df1]),
            sqlalchemy_exc.OperationalError(
                "statement", "params",
                orig=Exception("SerializationFailure: terminating connection"),
                connection_invalidated=False
            ),
            iter([df2])
        ]

        row1 = {'query': 'SELECT * FROM table1', 'prefix': 'prefix1', 'chunksize': 1000}
        row2 = {'query': 'SELECT * FROM table2', 'prefix': 'prefix2', 'chunksize': 1000}

        result1 = _dump_with_retry(row1, mock_db, mock_s3, dump_count=1)
        result2 = _dump_with_retry(row2, mock_db, mock_s3, dump_count=2)

        assert result1 is True, "First dump should succeed"
        assert result2 is True, "Second dump should succeed on retry"
        assert mock_db.execute_query.call_count == 3, \
            "Should call execute_query 3 times (dump1, dump2 fail, dump2 retry)"
        assert mock_db.rollback.call_count == 1, "Should rollback once for dump2"
        assert mock_db.commit.call_count == 2, "Should commit twice (dump1 success, dump2 retry success)"
        assert mock_s3.cleanup.call_count == 1, "Should cleanup S3 once before dump2 retry"
        assert mock_s3.write_parquet.call_count == 2, \
            "Should write twice (dump1, dump2 retry)"


@pytest.mark.standalone
class TestS3BucketFallback:
    """Test that main() retries list_directories with a trailing slash on ClientError."""

    @pytest.fixture(autouse=True)
    def setup_env(self):
        with open('tests/env.yaml', 'r') as stream:
            settings = yaml.safe_load(stream)
            for key in settings:
                environ[key] = settings[key]

    @patch('floorist.floorist._dump_with_retry')
    @patch('floorist.floorist.create_engine')
    @patch('floorist.floorist.wr.s3.list_directories')
    def test_list_directories_retries_with_trailing_slash(
        self,
        mock_s3_list,
        mock_create_engine,
        mock_dump_with_retry,
    ):
        mock_s3_list.side_effect = [
            botocore.exceptions.ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "ListBuckets",
            ),
            [],
        ]

        mock_conn = Mock()
        mock_engine = Mock()
        mock_engine.connect.return_value.execution_options.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        mock_dump_with_retry.return_value = True

        main()

        assert mock_s3_list.call_count == 2
        mock_s3_list.assert_any_call("s3://floorist")
        mock_s3_list.assert_any_call("s3://floorist/")
        mock_dump_with_retry.assert_called()

    @patch('floorist.floorist._dump_with_retry')
    @patch('floorist.floorist.wr.s3.list_directories')
    def test_list_directories_does_not_retry_on_non_access_denied_client_error(
        self,
        mock_s3_list,
        mock_dump_with_retry,
    ):
        # Simulate a non-AccessDenied ClientError from S3, e.g. NoSuchBucket
        mock_s3_list.side_effect = botocore.exceptions.ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchBucket",
                    "Message": "The specified bucket does not exist",
                }
            },
            operation_name="ListDirectories",
        )
        # main() should propagate the error as-is and not attempt a retry
        with pytest.raises(botocore.exceptions.ClientError) as excinfo:
            main()
        # Ensure the exact error code is preserved
        assert excinfo.value.response["Error"]["Code"] == "NoSuchBucket"
        # list_directories should be called exactly once and not retried
        assert mock_s3_list.call_count == 1
        # _dump_with_retry must not be invoked for non-AccessDenied errors
        mock_dump_with_retry.assert_not_called()

    @patch('floorist.floorist._dump_with_retry')
    @patch('floorist.floorist.wr.s3.list_directories')
    def test_list_directories_retries_with_trailing_slash_and_fails(
        self,
        mock_s3_list,
        mock_dump_with_retry,
    ):
        mock_s3_list.side_effect = [
            botocore.exceptions.ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "ListBuckets",
            ),
            botocore.exceptions.ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
                "ListBuckets",
            ),
        ]

        with pytest.raises(botocore.exceptions.ClientError):
            main()

        assert mock_s3_list.call_count == 2
        mock_s3_list.assert_any_call("s3://floorist")
        mock_s3_list.assert_any_call("s3://floorist/")
        mock_dump_with_retry.assert_not_called()


@pytest.mark.standalone
class TestTransactionIsolation:
    @patch('floorist.floorist.pd.read_sql')
    @patch('floorist.floorist.boto3.setup_default_session')
    @patch('floorist.floorist.yaml.safe_load')
    @patch('floorist.floorist.open', new_callable=mock_open)
    @patch('floorist.floorist.create_engine')
    @patch('floorist.floorist.get_config')
    @patch('floorist.floorist.wr.s3.list_directories')
    @patch('floorist.floorist.wr.s3.to_parquet')
    def test_each_query_commits_separately(
        self,
        mock_s3_write,
        mock_s3_list,
        mock_get_config,
        mock_create_engine,
        mock_file,
        mock_yaml_load,
        mock_boto_session,
        mock_read_sql,
    ):
        mock_config = Mock(
            bucket_name="test-bucket",
            bucket_url="http://localhost:9000",
            bucket_access_key="access",
            bucket_secret_key="secret",
            bucket_region="us-east-1",
            floorplan_filename="test.yaml",
        )
        mock_get_config.return_value = mock_config

        call_order = []

        # Setup mock connection with commit tracking
        mock_conn = Mock()
        mock_conn.commit.side_effect = lambda: call_order.append('commit')

        mock_engine = Mock()
        mock_engine.connect.return_value.execution_options.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        floorplan_rows = [
            {'query': 'SELECT * FROM table1', 'prefix': 'prefix1'},
            {'query': 'SELECT * FROM table2', 'prefix': 'prefix2'},
        ]
        mock_yaml_load.return_value = floorplan_rows

        def track_read_sql(*args, **kwargs):
            call_order.append('read_sql')
            return [pd.DataFrame({'id': [1]})]

        mock_read_sql.side_effect = track_read_sql

        # Run main
        main()

        # Expect: read_sql, commit, read_sql, commit (interleaved)
        # NOT: read_sql, read_sql, commit, commit (batched)
        assert call_order == ['read_sql', 'commit', 'read_sql', 'commit'], (
            f"Expected interleaved pattern, got: {call_order}"
        )
