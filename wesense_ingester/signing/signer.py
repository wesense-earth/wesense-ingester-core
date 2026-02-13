"""
Reading signer and verifier.

Wraps raw payloads in a SignedReading protobuf envelope with an Ed25519
signature, and verifies signatures from other ingesters.
"""

import logging

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from wesense_ingester.proto.signed_reading_pb2 import SignedReading
from wesense_ingester.signing.keys import IngesterKeyManager

logger = logging.getLogger(__name__)


class ReadingSigner:
    """Signs reading payloads and verifies signatures."""

    def __init__(self, key_manager: IngesterKeyManager):
        self._km = key_manager

    def sign(self, payload: bytes) -> SignedReading:
        """Sign a payload and wrap it in a SignedReading envelope."""
        signature = self._km.private_key.sign(payload)
        envelope = SignedReading(
            payload=payload,
            signature=signature,
            ingester_id=self._km.ingester_id,
            key_version=self._km.key_version,
        )
        return envelope

    @staticmethod
    def verify(signed_reading: SignedReading, public_key: Ed25519PublicKey) -> bool:
        """Verify the signature on a SignedReading envelope."""
        try:
            public_key.verify(signed_reading.signature, signed_reading.payload)
            return True
        except InvalidSignature:
            return False

    @staticmethod
    def deserialize(data: bytes) -> SignedReading:
        """Parse serialized bytes into a SignedReading."""
        msg = SignedReading()
        msg.ParseFromString(data)
        return msg
