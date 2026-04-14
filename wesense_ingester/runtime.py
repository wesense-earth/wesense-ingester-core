"""
Runtime helpers for ingesters: signal handling for clean shutdown.

Consolidates the SIGINT/SIGTERM boilerplate that every ingester
previously duplicated. Works the same way for every ingester pattern
(polling, MQTT subscriber, webhook server, async, whatever).
"""

import logging
import signal
import threading

logger = logging.getLogger(__name__)


class Shutdown:
    """
    Shutdown coordination for ingesters.

    Installs SIGINT/SIGTERM handlers on construction. Sets `requested`
    when a signal arrives. `sleep(seconds)` returns early on shutdown.

    Typical usage:

        shutdown = Shutdown()
        # ... start background workers (MQTT subscribers, webhook server, etc.) ...
        while not shutdown.requested:
            # periodic work (stats, polling, etc.)
            shutdown.sleep(STATS_INTERVAL)
        # signal received — clean up
        self._cleanup()
    """

    def __init__(self, name: str = "ingester"):
        self._name = name
        self._requested = threading.Event()
        self._install_handlers()

    def _install_handlers(self) -> None:
        """Install SIGINT and SIGTERM handlers."""
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except ValueError:
            # signal.signal only works on the main thread; skip silently
            # when constructed from a worker thread.
            logger.debug("Shutdown handlers skipped (not main thread)")

    def _handle_signal(self, signum, frame) -> None:
        logger.info("%s: received signal %d, shutting down", self._name, signum)
        self._requested.set()

    @property
    def requested(self) -> bool:
        """True once a shutdown signal has been received."""
        return self._requested.is_set()

    def request(self) -> None:
        """Programmatically request shutdown (equivalent to receiving SIGTERM)."""
        self._requested.set()

    def sleep(self, seconds: float) -> bool:
        """
        Sleep for up to `seconds`, returning early if shutdown is requested.

        Returns True if shutdown was requested during the sleep.
        """
        return self._requested.wait(timeout=seconds)

    def wait(self) -> None:
        """Block until shutdown is requested."""
        self._requested.wait()
