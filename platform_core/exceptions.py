from __future__ import annotations


class PlatformCoreError(Exception):
    pass


class TenantContextError(PlatformCoreError):
    pass


class AuthorizationError(PlatformCoreError):
    pass


class BootstrapError(PlatformCoreError):
    pass
