from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken

from platform_core.exceptions import PlatformCoreError
from platform_core.settings import PlatformSettings, load_platform_settings


class CredentialCryptoError(PlatformCoreError):
    pass


def _derive_fernet_key(settings: PlatformSettings) -> bytes:
    raw = (settings.credentials_key or settings.secret_key).encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    return base64.urlsafe_b64encode(digest)


@dataclass
class CredentialCrypto:
    fernet: Fernet

    @classmethod
    def from_settings(cls, settings: PlatformSettings | None = None) -> "CredentialCrypto":
        resolved = settings or load_platform_settings()
        return cls(fernet=Fernet(_derive_fernet_key(resolved)))

    def encrypt_mapping(self, payload: dict[str, object]) -> tuple[str, str]:
        plaintext = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ciphertext = self.fernet.encrypt(plaintext).decode("utf-8")
        fingerprint = hashlib.sha256(plaintext).hexdigest()
        return ciphertext, fingerprint

    def decrypt_mapping(self, ciphertext: str) -> dict[str, object]:
        try:
            plaintext = self.fernet.decrypt(ciphertext.encode("utf-8"))
        except InvalidToken as exc:
            raise CredentialCryptoError("Credential payload could not be decrypted with the current key.") from exc
        return json.loads(plaintext.decode("utf-8"))
