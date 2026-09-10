"""Salesforce Pub/Sub API client for asyncio"""

import logging
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

from .auth import (
    AuthenticatorBase,
    ClientCredentialsAuthenticator,
    PasswordAuthenticator,
    RefreshTokenAuthenticator,
)
from .cdc import (
    expand_bitmap,
    expand_bitmap_fields,
    expand_change_event_header,
)
from .client import (
    ManagedSubscription,
    ReplayMarkerStoragePolicy,
    SalesforcePubSubClient,
)
from .exceptions import (
    AuthenticationError,
    ClientError,
    ClientInvalidOperation,
    PublishError,
    PubSubException,
    ReplayError,
    SchemaError,
)
from .replay import (
    ConstantReplayId,
    MappingStorage,
    ReplayMarkerStorage,
    ReplayOption,
)

try:
    __version__ = distribution_version("simple-salesforce-pubsub")
except PackageNotFoundError:  # pragma: no cover - source checkout
    __version__ = "0.0.0"

__all__ = [
    "AuthenticationError",
    "AuthenticatorBase",
    "ClientCredentialsAuthenticator",
    "ClientError",
    "ClientInvalidOperation",
    "ConstantReplayId",
    "ManagedSubscription",
    "MappingStorage",
    "PasswordAuthenticator",
    "PubSubException",
    "PublishError",
    "RefreshTokenAuthenticator",
    "ReplayError",
    "ReplayMarkerStorage",
    "ReplayMarkerStoragePolicy",
    "ReplayOption",
    "SalesforcePubSubClient",
    "SchemaError",
    "__version__",
    "expand_bitmap",
    "expand_bitmap_fields",
    "expand_change_event_header",
]

# Create a default handler to avoid warnings in applications without logging
# configuration
logging.getLogger(__name__).addHandler(logging.NullHandler())
