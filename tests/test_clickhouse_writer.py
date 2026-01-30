"""Tests for buffered ClickHouse writer."""

from unittest.mock import MagicMock, patch


def _make_writer(batch_size=100, flush_interval=9999):
    """Create a writer with mocked clickhouse_connect."""
    mock_client = MagicMock()
    mock_ch = MagicMock()
    mock_ch.get_client.return_value = mock_client

    with patch.dict("sys.modules", {"clickhouse_connect": mock_ch}):
        import importlib
        import wesense_ingester.clickhouse.writer as writer_mod
        importlib.reload(writer_mod)

        from wesense_ingester.clickhouse.writer import BufferedClickHouseWriter, ClickHouseConfig

        config = ClickHouseConfig(
            host="localhost", port=8123, user="default",
            password="", database="test_db", table="test_table",
        )
        writer = BufferedClickHouseWriter(
            config=config,
            columns=["timestamp", "device_id", "value"],
            batch_size=batch_size,
            flush_interval=flush_interval,
        )
        return writer, mock_client


def test_add_row_to_buffer():
    writer, mock_client = _make_writer()
    try:
        writer.add(("2024-01-01", "sensor-1", 22.5))

        stats = writer.get_stats()
        assert stats["buffer_size"] == 1
        assert stats["total_written"] == 0
    finally:
        writer.close()


def test_flush_writes_to_clickhouse():
    writer, mock_client = _make_writer()
    try:
        writer.add(("2024-01-01", "sensor-1", 22.5))
        writer.flush()

        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        assert call_args[0][0] == "test_db.test_table"
        assert len(call_args[0][1]) == 1

        stats = writer.get_stats()
        assert stats["buffer_size"] == 0
        assert stats["total_written"] == 1
    finally:
        writer.close()


def test_batch_size_triggers_flush():
    writer, mock_client = _make_writer(batch_size=3)
    try:
        writer.add(("2024-01-01", "s1", 1.0))
        writer.add(("2024-01-01", "s2", 2.0))
        assert mock_client.insert.call_count == 0

        writer.add(("2024-01-01", "s3", 3.0))
        assert mock_client.insert.call_count == 1

        stats = writer.get_stats()
        assert stats["total_written"] == 3
    finally:
        writer.close()


def test_retry_on_failure():
    writer, mock_client = _make_writer()
    try:
        mock_client.insert.side_effect = Exception("Connection lost")

        writer.add(("2024-01-01", "sensor-1", 22.5))
        writer.flush()

        # Rows should be returned to buffer
        stats = writer.get_stats()
        assert stats["buffer_size"] == 1
        assert stats["total_failed"] == 1

        # Fix the connection and retry
        mock_client.insert.side_effect = None
        writer.flush()

        stats = writer.get_stats()
        assert stats["buffer_size"] == 0
        assert stats["total_written"] == 1
    finally:
        writer.close()


def test_empty_flush_no_op():
    writer, mock_client = _make_writer()
    try:
        writer.flush()
        assert mock_client.insert.call_count == 0
    finally:
        writer.close()


def test_columns_passed_to_insert():
    writer, mock_client = _make_writer()
    try:
        writer.add(("2024-01-01", "sensor-1", 22.5))
        writer.flush()

        call_kwargs = mock_client.insert.call_args[1]
        assert call_kwargs["column_names"] == ["timestamp", "device_id", "value"]
    finally:
        writer.close()
