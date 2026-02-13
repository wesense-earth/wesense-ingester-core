"""
Integration tests for Zenoh publish/subscribe roundtrip.

Requires:
  - Docker: eclipse/zenoh:1.7.2 running on localhost:7447
  - pip install -e ".[p2p]" (eclipse-zenoh + cryptography + protobuf)

Run:
  docker run -d --name zenohd-test -p 7447:7447 eclipse/zenoh:1.7.2
  pytest tests/integration/test_zenoh_roundtrip.py -v -m integration
  docker stop zenohd-test && docker rm zenohd-test
"""

import json
import time

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture
def key_manager():
    """Generate a fresh Ed25519 keypair for testing."""
    from wesense_ingester.signing.keys import IngesterKeyManager, KeyConfig
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmpdir:
        config = KeyConfig(key_dir=tmpdir, key_file="test_key.pem")
        km = IngesterKeyManager(config=config)
        km.load_or_generate()
        yield km


@pytest.fixture
def trust_store(key_manager):
    """Create a trust store containing the test key."""
    from wesense_ingester.signing.trust import TrustStore
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        store = TrustStore(trust_file=f"{tmpdir}/trust.json")
        store.add_trusted(
            key_manager.ingester_id,
            key_manager.public_key_bytes,
            key_manager.key_version,
        )
        yield store


@pytest.fixture
def signer(key_manager):
    """Create a ReadingSigner with the test keypair."""
    from wesense_ingester.signing.signer import ReadingSigner
    return ReadingSigner(key_manager)


@pytest.fixture
def zenoh_config():
    """Config pointing to local zenohd on default port."""
    from wesense_ingester.zenoh.config import ZenohConfig
    return ZenohConfig(
        mode="client",
        routers=["tcp/localhost:7447"],
        enabled=True,
        key_prefix="wesense/test",
    )


def test_publish_subscribe_roundtrip(zenoh_config, signer, trust_store):
    """Publish a signed reading, subscriber receives and verifies it."""
    from wesense_ingester.zenoh.publisher import ZenohPublisher
    from wesense_ingester.zenoh.subscriber import ZenohSubscriber

    received = []

    def on_reading(reading_dict, signed_reading):
        received.append((reading_dict, signed_reading))

    subscriber = ZenohSubscriber(
        config=zenoh_config,
        trust_store=trust_store,
        on_reading=on_reading,
    )
    subscriber.connect()
    assert subscriber.is_connected()
    subscriber.subscribe("wesense/test/**")

    time.sleep(0.5)  # Let subscription propagate

    publisher = ZenohPublisher(config=zenoh_config, signer=signer)
    publisher._connect_worker()  # Blocking connect for test
    assert publisher.is_connected()

    reading = {
        "device_id": "integration-sensor",
        "data_source": "TEST",
        "geo_country": "nz",
        "geo_subdivision": "auk",
        "reading_type": "temperature",
        "value": 22.5,
    }

    result = publisher.publish_reading(reading)
    assert result is True

    time.sleep(1.0)  # Wait for delivery

    assert len(received) == 1
    delivered_reading, delivered_signed = received[0]
    assert delivered_reading["device_id"] == "integration-sensor"
    assert delivered_reading["value"] == 22.5
    assert delivered_signed is not None
    assert delivered_signed.ingester_id == signer._km.ingester_id

    assert subscriber.stats["verified"] == 1
    assert subscriber.stats["rejected"] == 0

    publisher.close()
    subscriber.close()


def test_untrusted_publisher_rejected(zenoh_config, signer):
    """Publisher key not in trust store — callback should NOT be called."""
    from wesense_ingester.zenoh.publisher import ZenohPublisher
    from wesense_ingester.zenoh.subscriber import ZenohSubscriber
    from wesense_ingester.signing.trust import TrustStore
    import tempfile

    # Empty trust store — no keys trusted
    with tempfile.TemporaryDirectory() as tmpdir:
        empty_trust = TrustStore(trust_file=f"{tmpdir}/trust.json")

        received = []

        def on_reading(reading_dict, signed_reading):
            received.append((reading_dict, signed_reading))

        subscriber = ZenohSubscriber(
            config=zenoh_config,
            trust_store=empty_trust,
            on_reading=on_reading,
        )
        subscriber.connect()
        subscriber.subscribe("wesense/test/**")

        time.sleep(0.5)

        publisher = ZenohPublisher(config=zenoh_config, signer=signer)
        publisher._connect_worker()

        reading = {
            "device_id": "untrusted-sensor",
            "data_source": "TEST",
            "geo_country": "nz",
            "geo_subdivision": "auk",
            "reading_type": "temperature",
            "value": 10.0,
        }

        publisher.publish_reading(reading)

        time.sleep(1.0)

        assert len(received) == 0
        assert subscriber.stats["rejected"] == 1

        publisher.close()
        subscriber.close()


def test_queryable_roundtrip(zenoh_config):
    """Register queryable, send query, verify response."""
    import zenoh
    from wesense_ingester.zenoh.queryable import ZenohQueryable

    mock_ch_config = type("Config", (), {
        "host": "localhost", "port": 8123, "user": "test",
        "password": "", "database": "wesense", "table": "sensor_readings",
    })()

    queryable = ZenohQueryable(config=zenoh_config, clickhouse_config=mock_ch_config)
    queryable.connect()

    # Without a real ClickHouse, we get a "no clickhouse" error response
    # which still validates the roundtrip
    queryable._ch_client = None
    queryable.register("wesense/test/query/**")

    time.sleep(0.5)

    # Open a separate session to send a query
    cfg = zenoh.Config.from_json5(zenoh_config.to_zenoh_json())
    session = zenoh.open(cfg)

    replies = session.get("wesense/test/query/**", value=b"summary")
    results = []
    for reply in replies:
        if reply.ok:
            data = json.loads(bytes(reply.ok.payload))
            results.append(data)

    assert len(results) >= 1
    assert "error" in results[0]  # No ClickHouse = error response

    session.close()
    queryable.close()
