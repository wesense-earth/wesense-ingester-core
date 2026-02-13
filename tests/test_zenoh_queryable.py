"""Tests for ZenohQueryable — zenoh and clickhouse mocked."""

import importlib
import json
import sys
import threading
from unittest.mock import MagicMock, patch, call

from wesense_ingester.zenoh.config import ZenohConfig


def _make_ch_config():
    """Create a mock ClickHouseConfig."""
    config = MagicMock()
    config.host = "localhost"
    config.port = 8123
    config.user = "wesense"
    config.password = "pass"
    config.database = "wesense"
    config.table = "sensor_readings"
    return config


def _make_queryable(ch_config=None, config=None):
    """Create a ZenohQueryable with mocked zenoh and clickhouse."""
    mock_zenoh = MagicMock()
    mock_session = MagicMock()
    mock_zenoh.open.return_value = mock_session
    mock_zenoh.Config.from_json5.return_value = MagicMock()

    mock_ch = MagicMock()
    mock_ch_client = MagicMock()

    with patch.dict(sys.modules, {"zenoh": mock_zenoh, "clickhouse_connect": mock_ch}):
        import wesense_ingester.zenoh.queryable as q_mod
        importlib.reload(q_mod)

        cfg = config or ZenohConfig(
            mode="client",
            routers=["tcp/localhost:7447"],
            enabled=True,
        )
        ch_cfg = ch_config or _make_ch_config()
        queryable = q_mod.ZenohQueryable(config=cfg, clickhouse_config=ch_cfg)
        queryable._session = mock_session
        queryable._connected = True
        queryable._ch_client = mock_ch_client

        return queryable, mock_session, mock_ch_client, mock_zenoh


def _make_query_result(column_names, rows):
    """Create a mock ClickHouse query result."""
    result = MagicMock()
    result.column_names = column_names
    result.result_rows = rows
    return result


def test_register_declares_queryable():
    queryable, mock_session, _, _ = _make_queryable()
    mock_qable = MagicMock()
    mock_session.declare_queryable.return_value = mock_qable

    queryable.register("wesense/v2/live/**")

    mock_session.declare_queryable.assert_called_once()
    args = mock_session.declare_queryable.call_args
    assert args[0][0] == "wesense/v2/live/**"


def test_on_query_dispatches_to_background_thread():
    queryable, _, _, _ = _make_queryable()

    mock_query = MagicMock()
    mock_query.payload = b"summary"

    with patch.object(threading, "Thread") as MockThread:
        mock_thread = MagicMock()
        MockThread.return_value = mock_thread

        queryable._on_query(mock_query)

        MockThread.assert_called_once()
        kwargs = MockThread.call_args[1]
        assert kwargs["target"] == queryable._handle_query
        assert kwargs["args"] == (mock_query,)
        assert kwargs["daemon"] is True
        mock_thread.start.assert_called_once()


def test_handle_summary_query():
    queryable, _, mock_ch_client, _ = _make_queryable()

    mock_ch_client.query.return_value = _make_query_result(
        ["geo_country", "geo_subdivision", "reading_type", "avg_value", "reading_count"],
        [("nz", "auk", "temperature", 22.5, 100)],
    )

    mock_query = MagicMock()
    mock_query.payload = b"summary"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    mock_ch_client.query.assert_called_once()
    sql = mock_ch_client.query.call_args[0][0]
    assert "GROUP BY" in sql
    assert "avg(value)" in sql

    mock_query.reply.assert_called_once()
    reply_data = json.loads(mock_query.reply.call_args[0][1])
    assert len(reply_data["results"]) == 1
    assert reply_data["results"][0]["geo_country"] == "nz"


def test_handle_latest_query():
    queryable, _, mock_ch_client, _ = _make_queryable()

    mock_ch_client.query.return_value = _make_query_result(
        ["device_id", "reading_type", "value"],
        [("sensor-001", "temperature", 22.5)],
    )

    mock_query = MagicMock()
    mock_query.payload = b"latest"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    sql = mock_ch_client.query.call_args[0][0]
    assert "LIMIT 1 BY" in sql


def test_handle_history_query_with_hours():
    queryable, _, mock_ch_client, _ = _make_queryable()

    mock_ch_client.query.return_value = _make_query_result(
        ["device_id", "value", "timestamp"],
        [("sensor-001", 22.5, "2026-02-13T10:00:00")],
    )

    mock_query = MagicMock()
    mock_query.payload = b"history?hours=6"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    sql = mock_ch_client.query.call_args[0][0]
    assert "INTERVAL 6 HOUR" in sql


def test_handle_history_query_caps_hours():
    queryable, _, mock_ch_client, _ = _make_queryable()

    mock_ch_client.query.return_value = _make_query_result([], [])

    mock_query = MagicMock()
    mock_query.payload = b"history?hours=999"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    sql = mock_ch_client.query.call_args[0][0]
    assert "INTERVAL 24 HOUR" in sql


def test_handle_devices_query():
    queryable, _, mock_ch_client, _ = _make_queryable()

    mock_ch_client.query.return_value = _make_query_result(
        ["device_id", "last_seen", "reading_count"],
        [("sensor-001", "2026-02-13T10:00:00", 42)],
    )

    mock_query = MagicMock()
    mock_query.payload = b"devices"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    sql = mock_ch_client.query.call_args[0][0]
    assert "GROUP BY device_id" in sql


def test_handle_unknown_query_type():
    queryable, _, _, _ = _make_queryable()

    mock_query = MagicMock()
    mock_query.payload = b"foobar"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    mock_query.reply.assert_called_once()
    reply_data = json.loads(mock_query.reply.call_args[0][1])
    assert "error" in reply_data
    assert "unknown query type" in reply_data["error"]


def test_query_without_clickhouse():
    queryable, _, _, _ = _make_queryable()
    queryable._ch_client = None

    mock_query = MagicMock()
    mock_query.payload = b"summary"
    mock_query.key_expr = "wesense/v2/live/**"

    queryable._handle_query(mock_query)

    mock_query.reply.assert_called_once()
    reply_data = json.loads(mock_query.reply.call_args[0][1])
    assert reply_data["results"] == []
    assert "no clickhouse" in reply_data["error"]


def test_register_when_not_connected():
    queryable, mock_session, _, _ = _make_queryable()
    queryable._connected = False

    queryable.register("wesense/v2/live/**")

    mock_session.declare_queryable.assert_not_called()


def test_close_undeclares_queryables():
    queryable, mock_session, _, _ = _make_queryable()

    mock_q1 = MagicMock()
    mock_q2 = MagicMock()
    queryable._queryables = [mock_q1, mock_q2]

    queryable.close()

    mock_q1.undeclare.assert_called_once()
    mock_q2.undeclare.assert_called_once()
    mock_session.close.assert_called_once()
    assert queryable._connected is False
    assert queryable._queryables == []


def test_connect_disabled():
    mock_zenoh = MagicMock()
    with patch.dict(sys.modules, {"zenoh": mock_zenoh, "clickhouse_connect": MagicMock()}):
        import wesense_ingester.zenoh.queryable as q_mod
        importlib.reload(q_mod)

        config = ZenohConfig(enabled=False)
        queryable = q_mod.ZenohQueryable(config=config)
        queryable.connect()

        mock_zenoh.open.assert_not_called()
        assert queryable.is_connected() is False
