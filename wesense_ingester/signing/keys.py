"""
Ed25519 key management for ingester identity.

Generates and persists an Ed25519 keypair used to sign readings.
Each ingester has a stable identity derived from its public key.
"""

import hashlib
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
    load_pem_private_key,
)

logger = logging.getLogger(__name__)


@dataclass
class KeyConfig:
    """Key storage configuration."""

    key_dir: str = "data/keys"
    key_file: str = "ingester_key.pem"

    @classmethod
    def from_env(cls) -> "KeyConfig":
        """Create config from environment variables with sensible defaults."""
        return cls(
            key_dir=os.getenv("ZENOH_KEY_DIR", "data/keys"),
            key_file=os.getenv("ZENOH_KEY_FILE", "ingester_key.pem"),
        )

    @property
    def pem_path(self) -> str:
        return os.path.join(self.key_dir, self.key_file)

    @property
    def sidecar_path(self) -> str:
        base, _ = os.path.splitext(self.pem_path)
        return base + ".json"


class IngesterKeyManager:
    """
    Manages an Ed25519 keypair for ingester identity and signing.

    On first run, generates a new keypair and saves it to disk.
    On subsequent runs, loads the existing keypair.
    """

    def __init__(self, config: KeyConfig | None = None):
        self._config = config or KeyConfig.from_env()
        self._private_key: Ed25519PrivateKey | None = None
        self._key_version: int = 1

    def load_or_generate(self) -> None:
        """Load existing keypair from disk, or generate a new one."""
        if os.path.exists(self._config.pem_path):
            self._load()
        else:
            self._generate()

    def _load(self) -> None:
        """Load keypair from PEM file and sidecar."""
        with open(self._config.pem_path, "rb") as f:
            self._private_key = load_pem_private_key(f.read(), password=None)

        if os.path.exists(self._config.sidecar_path):
            with open(self._config.sidecar_path, "r") as f:
                sidecar = json.load(f)
            self._key_version = sidecar.get("key_version", 1)

        logger.info(
            "Loaded keypair %s (version %d) from %s",
            self.ingester_id, self._key_version, self._config.pem_path,
        )

    def _generate(self) -> None:
        """Generate a new Ed25519 keypair and save to disk."""
        self._private_key = Ed25519PrivateKey.generate()
        self._key_version = 1

        os.makedirs(self._config.key_dir, exist_ok=True)

        # Atomic write PEM
        pem_bytes = self._private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        )
        self._atomic_write(self._config.pem_path, pem_bytes)

        # Atomic write JSON sidecar
        sidecar = {
            "key_version": self._key_version,
            "created": datetime.now(timezone.utc).isoformat(),
        }
        self._atomic_write(
            self._config.sidecar_path,
            json.dumps(sidecar, indent=2).encode(),
        )

        logger.info(
            "Generated new keypair %s (version %d) at %s",
            self.ingester_id, self._key_version, self._config.pem_path,
        )

    def _atomic_write(self, path: str, data: bytes) -> None:
        """Write data to path atomically using tempfile + os.replace."""
        dir_path = os.path.dirname(path)
        fd, tmp_path = tempfile.mkstemp(dir=dir_path or ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @property
    def private_key(self) -> Ed25519PrivateKey:
        if self._private_key is None:
            raise RuntimeError("Keys not loaded. Call load_or_generate() first.")
        return self._private_key

    @property
    def public_key(self) -> Ed25519PublicKey:
        return self.private_key.public_key()

    @property
    def public_key_bytes(self) -> bytes:
        """Raw 32-byte public key."""
        return self.public_key.public_bytes(
            encoding=Encoding.Raw,
            format=PublicFormat.Raw,
        )

    @property
    def ingester_id(self) -> str:
        """Deterministic ID: 'wsi_' + first 8 hex chars of SHA-256 of public key."""
        digest = hashlib.sha256(self.public_key_bytes).hexdigest()
        return f"wsi_{digest[:8]}"

    @property
    def key_version(self) -> int:
        return self._key_version
