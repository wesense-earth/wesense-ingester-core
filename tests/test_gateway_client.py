"""Tests for GatewayClient."""

import json
from unittest.mock import MagicMock, patch

from wesense_ingester.gateway.client import GatewayClient
from wesense_ingester.gateway.config import GatewayConfig


def _make_client(batch_size=100, flush_interval=9999, max_buffer_size=10000):
    """Create a GatewayClient with a long flush interval (manual flush in tests)."""
    config = GatewayConfig(
        url="http://test-gateway:8080",
        batch_size=batch_size,
        flush_interval=flush_interval,
        max_buffer_size=max_buffer_size,
        timeout=5.0,
    )
    client = GatewayClient(config=config)
    return client


def _sample_reading(**overrides):
    """Create a sample reading dict."""
    reading = {
        "timestamp": 1700000000,
        "device_id": "wesense_test_001",
        "data_source": "WESENSE",
        "reading_type": "temperature",
        "value": 22.5,
        "unit": "°C",
        "latitude": -36.85,
        "longitude": 174.76,
    }
    reading.update(overrides)
    return reading


def _mock_urlopen_success(accepted=1, duplicates=0, errors=0):
    """Create a mock urlopen that returns a success response."""
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps({
        "accepted": accepted,
        "duplicates": duplicates,
        "errors": errors,
    }).encode()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


class TestGatewayConfig:

    def test_defaults(self):
        config = GatewayConfig()
        assert config.url == "http://localhost:8080"
        assert config.batch_size == 100
        assert config.flush_interval == 10.0
        assert config.max_buffer_size == 10000
        assert config.timeout == 10.0

    def test_from_env(self):
        env = {
            "GATEWAY_URL": "http://gw:9090",
            "GATEWAY_BATCH_SIZE": "50",
            "GATEWAY_FLUSH_INTERVAL": "5.0",
            "GATEWAY_MAX_BUFFER_SIZE": "5000",
            "GATEWAY_TIMEOUT": "15.0",
        }
        with patch.dict("os.environ", env):
            config = GatewayConfig.from_env()
        assert config.url == "http://gw:9090"
        assert config.batch_size == 50
        assert config.flush_interval == 5.0
        assert config.max_buffer_size == 5000
        assert config.timeout == 15.0


class TestGatewayClientAdd:

    def test_add_buffers_reading(self):
        client = _make_client()
        try:
            client.add(_sample_reading())
            stats = client.get_stats()
            assert stats["buffer_size"] == 1
            assert stats["total_sent"] == 0
        finally:
            # Suppress flush on close
            client._flush_timer.cancel()
            client._flush_timer = None

    def test_batch_size_triggers_auto_flush(self):
        client = _make_client(batch_size=2)
        try:
            mock_resp = _mock_urlopen_success(accepted=2)
            with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
                client.add(_sample_reading(device_id="d1"))
                assert client.get_stats()["buffer_size"] == 1
                client.add(_sample_reading(device_id="d2"))

                # Auto-flush should have been triggered
                mock_open.assert_called_once()
                assert client.get_stats()["buffer_size"] == 0
                assert client.get_stats()["total_sent"] == 2
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None


class TestGatewayClientFlush:

    def test_flush_sends_correct_json(self):
        client = _make_client()
        try:
            reading = _sample_reading()
            client.add(reading)

            mock_resp = _mock_urlopen_success(accepted=1)
            with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
                client.flush()

                # Verify the request
                mock_open.assert_called_once()
                req = mock_open.call_args[0][0]
                assert req.full_url == "http://test-gateway:8080/readings"
                assert req.method == "POST"
                assert req.get_header("Content-type") == "application/json"

                body = json.loads(req.data.decode())
                assert "readings" in body
                assert len(body["readings"]) == 1
                assert body["readings"][0]["device_id"] == "wesense_test_001"
                assert body["readings"][0]["value"] == 22.5
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None

    def test_empty_flush_is_noop(self):
        client = _make_client()
        try:
            with patch("urllib.request.urlopen") as mock_open:
                client.flush()
                mock_open.assert_not_called()
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None

    def test_flush_failure_returns_readings_to_buffer(self):
        import urllib.error
        client = _make_client()
        try:
            client.add(_sample_reading())
            client.add(_sample_reading(device_id="d2"))

            with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("connection refused")):
                client.flush()

            stats = client.get_stats()
            # Readings should be back in the buffer
            assert stats["buffer_size"] == 2
            assert stats["total_failed"] == 2
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None

    def test_max_buffer_overflow_drops_oldest(self):
        import urllib.error
        client = _make_client(max_buffer_size=3)
        try:
            # Add 3 readings (fills buffer to max)
            client.add(_sample_reading(device_id="d1"))
            client.add(_sample_reading(device_id="d2"))
            client.add(_sample_reading(device_id="d3"))

            # Fail the flush — all 3 return to buffer
            with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("timeout")):
                client.flush()

            assert client.get_stats()["buffer_size"] == 3

            # Add 2 more readings to the buffer
            client.add(_sample_reading(device_id="d4"))
            client.add(_sample_reading(device_id="d5"))

            # Fail flush again — 5 readings try to go back but max is 3
            with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("timeout")):
                client.flush()

            stats = client.get_stats()
            assert stats["buffer_size"] == 3

            # Verify the oldest were dropped (d3, d4, d5 should remain)
            with client._lock:
                ids = [r["device_id"] for r in client._buffer]
            assert ids == ["d3", "d4", "d5"]
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None


class TestGatewayClientStats:

    def test_stats_tracking(self):
        client = _make_client()
        try:
            client.add(_sample_reading())
            client.add(_sample_reading())

            mock_resp = _mock_urlopen_success(accepted=2)
            with patch("urllib.request.urlopen", return_value=mock_resp):
                client.flush()

            stats = client.get_stats()
            assert stats["buffer_size"] == 0
            assert stats["total_sent"] == 2
            assert stats["total_written"] == 2  # alias
            assert stats["total_failed"] == 0
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None

    def test_duplicates_counted_as_sent(self):
        client = _make_client()
        try:
            client.add(_sample_reading())

            mock_resp = _mock_urlopen_success(accepted=0, duplicates=1)
            with patch("urllib.request.urlopen", return_value=mock_resp):
                client.flush()

            stats = client.get_stats()
            assert stats["total_sent"] == 1
        finally:
            client._flush_timer.cancel()
            client._flush_timer = None


class TestGatewayClientClose:

    def test_close_flushes_remaining(self):
        client = _make_client()
        client.add(_sample_reading())

        mock_resp = _mock_urlopen_success(accepted=1)
        with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
            client.close()
            mock_open.assert_called_once()

        assert client.get_stats()["total_sent"] == 1
        assert client._flush_timer is None
