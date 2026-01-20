import pandas as pd
import pytest

from floorist.floorist import main, _is_retryable_db_error, _safe_rollback, _cleanup_s3_target, _dump_with_retry, MAX_RETRIES, RETRY_DELAY
from sqlalchemy import exc as sqlalchemy_exc
from unittest.mock import Mock, patch, MagicMock, mock_open


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


@pytest.mark.standalone
class TestTransactionIsolation:
    @patch('pandas.read_sql')
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
