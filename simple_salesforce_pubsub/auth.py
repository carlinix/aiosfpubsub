"""Authenticator class implementations"""

import reprlib
from abc import ABC, abstractmethod
from http import HTTPStatus
from typing import Any

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError as AiohttpClientError

from .exceptions import AuthenticationError

TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"
SANDBOX_TOKEN_URL = "https://test.salesforce.com/services/oauth2/token"


class AuthenticatorBase(ABC):
    """Abstract base class to serve as a base for implementing concrete
    authenticators"""

    def __init__(self, sandbox: bool = False) -> None:
        """
        :param sandbox: Marks whether the authentication has to be done \
        for a sandbox org or for a production org
        """
        #: Marks whether the authentication has to be done for a sandbox org \
        #: or for a production org
        self._sandbox = sandbox
        #: Salesforce session ID that can be used with the web services API
        self.access_token: str | None = None
        #: Value is Bearer for all responses that include an access token
        self.token_type: str | None = None
        #: A URL indicating the instance of the user's org
        self.instance_url: str | None = None
        #: Identity URL that can be used to both identify the user and query \
        #: for more information about the user
        self.id: str | None = None
        #: The org ID, extracted from the identity URL. The Pub/Sub API \
        #: requires it as the ``tenantid`` call metadata entry
        self.tenant_id: str | None = None

    @property
    def _token_url(self) -> str:
        """The URL that should be used for token requests"""
        if self._sandbox:
            return SANDBOX_TOKEN_URL
        return TOKEN_URL

    async def authenticate(self) -> None:
        """Called on initialization and after a failed authentication attempt

        :raise AuthenticationError: If the server rejects the authentication \
        request or if a network failure occurs
        """
        try:
            status_code, response_data = await self._authenticate()
        except AiohttpClientError as error:
            raise AuthenticationError("Network request failed") from error

        if status_code != HTTPStatus.OK:
            self.access_token = None
            self.token_type = None
            self.instance_url = None
            self.id = None
            self.tenant_id = None
            raise AuthenticationError("Authentication failed", response_data)

        self.access_token = response_data.get("access_token")
        self.token_type = response_data.get("token_type")
        self.instance_url = response_data.get("instance_url")
        self.id = response_data.get("id")
        self.tenant_id = self.get_tenant_id(self.id)

    @staticmethod
    def get_tenant_id(identity_url: str | None) -> str | None:
        """Extract the org ID from an *identity_url*

        The identity URL has the form
        ``https://login.salesforce.com/id/<org id>/<user id>``.

        :param identity_url: The ``id`` value of a token response
        :return: The org ID, or ``None`` if it can't be determined
        """
        if not identity_url:
            return None
        parts = identity_url.rstrip("/").split("/")
        if len(parts) < 2:
            return None
        return parts[-2]

    @abstractmethod
    async def _authenticate(self) -> tuple[int, dict[str, Any]]:
        """Authenticate the user

        :return: The status code and response data from the server's response
        :raise aiohttp.client_exceptions.ClientError: If a network failure \
        occurs
        """

    def get_grpc_metadata(self) -> tuple[tuple[str, str], ...]:
        """Return the call metadata required by every Pub/Sub API RPC

        :raise AuthenticationError: If called without authenticating first
        """
        if not self.access_token or not self.instance_url or not self.tenant_id:
            raise AuthenticationError(
                "Unknown access_token, instance_url or tenant_id values. "
                "Method called without authenticating first."
            )

        return (
            ("accesstoken", self.access_token),
            ("instanceurl", self.instance_url),
            ("tenantid", self.tenant_id),
        )


class PasswordAuthenticator(AuthenticatorBase):
    """Authenticator for using the OAuth 2.0 Username-Password Flow"""

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        username: str,
        password: str,
        sandbox: bool = False,
    ) -> None:
        """
        :param consumer_key: Consumer key from the Salesforce connected \
        app definition
        :param consumer_secret: Consumer secret from the Salesforce \
        connected app definition
        :param username: Salesforce username
        :param password: Salesforce password
        :param sandbox: Marks whether the authentication has to be done \
        for a sandbox org or for a production org
        """
        super().__init__(sandbox=sandbox)
        #: OAuth2 client id
        self.client_id = consumer_key
        #: OAuth2 client secret
        self.client_secret = consumer_secret
        #: Salesforce username
        self.username = username
        #: Salesforce password
        self.password = password

    def __repr__(self) -> str:
        """Formal string representation"""
        cls_name = type(self).__name__
        return (
            f"{cls_name}(consumer_key={reprlib.repr(self.client_id)}, "
            f"consumer_secret={reprlib.repr(self.client_secret)}, "
            f"username={reprlib.repr(self.username)}, "
            f"password={reprlib.repr(self.password)})"
        )

    async def _authenticate(self) -> tuple[int, dict[str, Any]]:
        async with ClientSession() as session:
            data = {
                "grant_type": "password",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "username": self.username,
                "password": self.password,
            }
            response = await session.post(self._token_url, data=data)
            return response.status, await response.json()


class RefreshTokenAuthenticator(AuthenticatorBase):
    """Authenticator for using the OAuth 2.0 Refresh Token Flow"""

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        refresh_token: str,
        sandbox: bool = False,
    ) -> None:
        """
        :param consumer_key: Consumer key from the Salesforce connected \
        app definition
        :param consumer_secret: Consumer secret from the Salesforce \
        connected app definition
        :param refresh_token: A refresh token obtained from Salesforce \
        by using one of its authentication methods
        :param sandbox: Marks whether the authentication has to be done \
        for a sandbox org or for a production org
        """
        super().__init__(sandbox=sandbox)
        #: OAuth2 client id
        self.client_id = consumer_key
        #: OAuth2 client secret
        self.client_secret = consumer_secret
        #: Salesforce refresh token
        self.refresh_token = refresh_token

    def __repr__(self) -> str:
        """Formal string representation"""
        cls_name = type(self).__name__
        return (
            f"{cls_name}(consumer_key={reprlib.repr(self.client_id)}, "
            f"consumer_secret={reprlib.repr(self.client_secret)}, "
            f"refresh_token={reprlib.repr(self.refresh_token)})"
        )

    async def _authenticate(self) -> tuple[int, dict[str, Any]]:
        async with ClientSession() as session:
            data = {
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            }
            response = await session.post(self._token_url, data=data)
            return response.status, await response.json()


class ClientCredentialsAuthenticator(AuthenticatorBase):
    """Authenticator for using the OAuth 2.0 Client Credentials Flow

    Unlike the username-password and refresh token flows, this flow sends no
    user credentials. The user that the integration acts as is configured on
    the Salesforce side, on the external client app or connected app.

    Salesforce only issues client credentials tokens from an org's My Domain
    host, so *domain* is required, and ``login`` and ``test`` are rejected.
    """

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        domain: str,
    ) -> None:
        """
        :param consumer_key: Consumer key from the Salesforce external \
        client app or connected app definition
        :param consumer_secret: Consumer secret from the Salesforce \
        external client app or connected app definition
        :param domain: The org's My Domain name, without a scheme and \
        without the ``.salesforce.com`` suffix, such as ``mycompany.my``
        :raise ValueError: If *domain* is empty, looks like a URL, carries \
        the ``.salesforce.com`` suffix, or is ``login`` or ``test``
        """
        super().__init__()
        #: OAuth2 client id
        self.client_id = consumer_key
        #: OAuth2 client secret
        self.client_secret = consumer_secret
        #: The org's My Domain name
        self.domain = self._validate_domain(domain)

    @staticmethod
    def _validate_domain(domain: str) -> str:
        """Check that *domain* can name a My Domain host

        The value is used to build the token URL, so a URL or a value with
        the ``.salesforce.com`` suffix would produce a malformed endpoint,
        and ``login``/``test`` would point at a host that does not serve
        this flow. Failing here gives a clearer error than a 404 later.

        :param domain: The domain value to check
        :return: The validated domain
        :raise ValueError: If the value cannot name a My Domain host
        """
        value = domain.strip().strip("/")
        if not value:
            raise ValueError("domain is required for the client credentials flow")
        if "://" in value:
            raise ValueError(
                f"domain must be a bare My Domain name, not a URL: {domain!r}"
            )
        if value.endswith(".salesforce.com"):
            raise ValueError(
                f"domain must not carry the .salesforce.com suffix: {domain!r}"
            )
        if value in ("login", "test"):
            raise ValueError(
                "the client credentials flow requires an "
                f"org's My Domain host, not {value!r}"
            )
        return value

    @property
    def _token_url(self) -> str:
        """The URL that should be used for token requests"""
        return f"https://{self.domain}.salesforce.com/services/oauth2/token"

    def __repr__(self) -> str:
        """Formal string representation"""
        cls_name = type(self).__name__
        return (
            f"{cls_name}(consumer_key={reprlib.repr(self.client_id)}, "
            f"consumer_secret={reprlib.repr(self.client_secret)}, "
            f"domain={reprlib.repr(self.domain)})"
        )

    async def _authenticate(self) -> tuple[int, dict[str, Any]]:
        async with ClientSession() as session:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }
            response = await session.post(self._token_url, data=data)
            return response.status, await response.json()
