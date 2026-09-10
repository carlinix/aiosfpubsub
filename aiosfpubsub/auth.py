"""Authenticator class implementations"""

import reprlib
import time
from abc import ABC, abstractmethod
from http import HTTPStatus
from os import PathLike
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from xml.etree import ElementTree
from xml.sax.saxutils import escape

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError as AiohttpClientError

from .exceptions import AuthenticationError

try:
    import jwt
except ImportError:  # pragma: no cover
    jwt = None  # type: ignore[assignment]

TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"
SANDBOX_TOKEN_URL = "https://test.salesforce.com/services/oauth2/token"
AUDIENCE_URL = "https://login.salesforce.com"
SANDBOX_AUDIENCE_URL = "https://test.salesforce.com"
JWT_BEARER_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"
JWT_ALGORITHM = "RS256"
#: Lifetime of a JWT assertion in seconds. The assertion is used once, right
#: after it is signed, so a short window costs nothing and limits the value of
#: a leaked assertion.
JWT_EXPIRATION = 180
LOGIN_DOMAIN = "login"
SANDBOX_LOGIN_DOMAIN = "test"
#: Salesforce API version of the SOAP login endpoint. ``login()`` is already
#: unavailable in version 65.0 and later, so the value has to stay below that
#: cutoff for the flow to work at all.
SOAP_API_VERSION = "59.0"
SOAP_LOGIN_ENVELOPE = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
    'xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">'
    "<env:Body>"
    '<n1:login xmlns:n1="urn:partner.soap.sforce.com">'
    "<n1:username>{username}</n1:username>"
    "<n1:password>{password}</n1:password>"
    "</n1:login>"
    "</env:Body>"
    "</env:Envelope>"
)


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
            self._clear_credentials()
            raise AuthenticationError("Authentication failed", response_data)

        self.access_token = response_data.get("access_token")
        self.token_type = response_data.get("token_type")
        self.instance_url = response_data.get("instance_url")
        self.id = response_data.get("id")
        self.tenant_id = self.get_tenant_id(self.id)

    def _clear_credentials(self) -> None:
        """Discard the values obtained from a previous authentication

        Called when an authentication attempt fails, so that a stale session
        can't outlive it and keep being presented as call metadata.
        """
        self.access_token = None
        self.token_type = None
        self.instance_url = None
        self.id = None
        self.tenant_id = None

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


class JWTBearerAuthenticator(AuthenticatorBase):
    """Authenticator for using the OAuth 2.0 JWT Bearer Flow

    No password and no client secret ever reach the wire. The client proves \
    its identity by signing a short lived assertion with the private key \
    whose certificate is uploaded to the Salesforce app definition, and the \
    user named by *username* has to be pre-authorized for that app.

    Signing requires `PyJWT <https://pyjwt.readthedocs.io/>`_ with its \
    cryptography backend, which is not a hard dependency of this package. \
    Install it with the ``jwt`` extra::

        pip install aiosfpubsub[jwt]
    """

    def __init__(
        self,
        consumer_key: str,
        username: str,
        private_key: str | bytes | None = None,
        private_key_path: str | PathLike[str] | None = None,
        audience: str | None = None,
        expiration: int = JWT_EXPIRATION,
        sandbox: bool = False,
    ) -> None:
        """
        :param consumer_key: Consumer key from the Salesforce external \
        client app or connected app definition
        :param username: Salesforce username of the user that the \
        integration acts as
        :param private_key: The RSA private key in PEM format, matching the \
        certificate uploaded to the app definition. Mutually exclusive with \
        *private_key_path*
        :param private_key_path: Path of a file holding the RSA private key \
        in PEM format. The file is read on initialization. Mutually \
        exclusive with *private_key*
        :param audience: Value of the assertion's ``aud`` claim. The default \
        is :py:data:`AUDIENCE_URL`, or :py:data:`SANDBOX_AUDIENCE_URL` if \
        *sandbox* is ``True``. Pass the site's URL to authenticate against \
        an Experience Cloud site
        :param expiration: Lifetime of the assertion in seconds
        :param sandbox: Marks whether the authentication has to be done \
        for a sandbox org or for a production org
        :raise ImportError: If PyJWT is not installed
        :raise ValueError: If neither or both of *private_key* and \
        *private_key_path* are given, or if *expiration* is not positive
        """
        if jwt is None:
            raise ImportError(
                "The JWT Bearer flow requires PyJWT with its cryptography "
                "backend. Install it with the 'jwt' extra: "
                "pip install aiosfpubsub[jwt]"
            )
        if (private_key is None) == (private_key_path is None):
            raise ValueError(
                "exactly one of private_key and private_key_path is required"
            )
        if expiration <= 0:
            raise ValueError(f"expiration must be positive, got {expiration!r}")

        super().__init__(sandbox=sandbox)
        #: OAuth2 client id
        self.client_id = consumer_key
        #: Salesforce username
        self.username = username
        #: The RSA private key in PEM format. Read eagerly from \
        #: *private_key_path*, so that a missing or unreadable key fails here \
        #: rather than on the first authentication attempt, and so that no \
        #: blocking file access happens while authenticating
        self.private_key: str | bytes = (
            private_key
            if private_key is not None
            else Path(private_key_path).read_bytes()  # type: ignore[arg-type]
        )
        #: Value of the assertion's ``aud`` claim
        self.audience = audience if audience is not None else self._default_audience
        #: Lifetime of the assertion in seconds
        self.expiration = expiration

    @property
    def _default_audience(self) -> str:
        """The audience matching the org that :py:attr:`~_token_url` points at"""
        if self._sandbox:
            return SANDBOX_AUDIENCE_URL
        return AUDIENCE_URL

    def __repr__(self) -> str:
        """Formal string representation

        The private key is omitted rather than shortened, since an
        abbreviated key would still disclose part of the secret.
        """
        cls_name = type(self).__name__
        return (
            f"{cls_name}(consumer_key={reprlib.repr(self.client_id)}, "
            f"username={reprlib.repr(self.username)}, "
            f"audience={reprlib.repr(self.audience)})"
        )

    def _create_assertion(self) -> str:
        """Create a signed JWT assertion for the token request

        :return: The encoded assertion
        """
        claims = {
            "iss": self.client_id,
            "sub": self.username,
            "aud": self.audience,
            "exp": int(time.time()) + self.expiration,
        }
        return jwt.encode(claims, self.private_key, algorithm=JWT_ALGORITHM)

    async def _authenticate(self) -> tuple[int, dict[str, Any]]:
        async with ClientSession() as session:
            data = {
                "grant_type": JWT_BEARER_GRANT_TYPE,
                "assertion": self._create_assertion(),
            }
            response = await session.post(self._token_url, data=data)
            return response.status, await response.json()


class SOAPAuthenticator(AuthenticatorBase):
    """Authenticator for using the SOAP API's ``login()`` call

    Unlike every OAuth flow, this one needs no connected app: it exchanges a
    username and a password for a session ID, which the Pub/Sub API accepts in
    place of an access token. It is the flow Salesforce's own Pub/Sub API
    reference client uses.

    That is its only advantage, and it comes with an expiry date. ``login()``
    is already unavailable in SOAP API version 65.0 and later, and Salesforce
    retires it from versions 31.0 through 64.0 in the Summer '27 release.
    Prefer :py:obj:`JWTBearerAuthenticator` or
    :py:obj:`ClientCredentialsAuthenticator` for new integrations, and reach
    for this class only when a connected app is genuinely out of reach.
    """

    def __init__(
        self,
        username: str,
        password: str,
        security_token: str = "",
        domain: str | None = None,
        sandbox: bool = False,
    ) -> None:
        """
        :param username: Salesforce username. To log in to a sandbox, the \
        name of the sandbox has to be appended to it, so a production \
        username of ``user@acme.com`` becomes ``user@acme.com.uat`` for a \
        sandbox named ``uat``
        :param password: Salesforce password
        :param security_token: The user's security token. It is required \
        unless the caller's IP falls inside the trusted IP range of the \
        user's profile. It is appended to *password*, which is what the \
        SOAP endpoint expects
        :param domain: The host to log in against, without a scheme and \
        without the ``.salesforce.com`` suffix, such as ``mycompany.my``. \
        The default is :py:data:`LOGIN_DOMAIN`, or \
        :py:data:`SANDBOX_LOGIN_DOMAIN` if *sandbox* is ``True``
        :param sandbox: Marks whether the authentication has to be done \
        for a sandbox org or for a production org
        :raise ValueError: If *domain* is empty, looks like a URL, or \
        carries the ``.salesforce.com`` suffix
        """
        super().__init__(sandbox=sandbox)
        #: Salesforce username
        self.username = username
        #: Salesforce password
        self.password = password
        #: The user's security token
        self.security_token = security_token
        #: The host that login requests are sent to
        self.domain = self._resolve_domain(domain, sandbox)

    @staticmethod
    def _resolve_domain(domain: str | None, sandbox: bool) -> str:
        """Determine the host to log in against

        :param domain: An explicit domain, or ``None`` to derive one from \
        *sandbox*
        :param sandbox: Marks whether a sandbox org is being addressed
        :return: The validated domain
        :raise ValueError: If *domain* cannot name a Salesforce host
        """
        if domain is None:
            return SANDBOX_LOGIN_DOMAIN if sandbox else LOGIN_DOMAIN

        value = domain.strip().strip("/")
        if not value:
            raise ValueError("domain must not be empty")
        if "://" in value:
            raise ValueError(
                f"domain must be a bare domain name, not a URL: {domain!r}"
            )
        if value.endswith(".salesforce.com"):
            raise ValueError(
                f"domain must not carry the .salesforce.com suffix: {domain!r}"
            )
        return value

    @property
    def _token_url(self) -> str:
        """The SOAP login endpoint that authentication requests are sent to"""
        return (
            f"https://{self.domain}.salesforce.com/services/Soap/u/{SOAP_API_VERSION}"
        )

    def __repr__(self) -> str:
        """Formal string representation

        The password and the security token are omitted rather than
        shortened, since an abbreviated secret would still disclose part of
        it.
        """
        cls_name = type(self).__name__
        return (
            f"{cls_name}(username={reprlib.repr(self.username)}, "
            f"domain={reprlib.repr(self.domain)})"
        )

    def _create_envelope(self) -> str:
        """Create the SOAP envelope of the login request

        :return: The envelope, with the credentials escaped for XML
        """
        return SOAP_LOGIN_ENVELOPE.format(
            username=escape(self.username),
            password=escape(self.password + self.security_token),
        )

    @staticmethod
    def _find_value(root: ElementTree.Element, name: str) -> str | None:
        """Find the text of the first element named *name*

        The response carries several namespaces which differ between a
        successful login and a fault, so elements are matched on their local
        name. Salesforce's own reference client indexes the response
        positionally instead, which breaks on any optional element.

        :param root: The root of the parsed response
        :param name: Local name of the element to look for
        :return: The element's text, or ``None`` if there is no such element
        """
        for element in root.iter():
            if element.tag.rpartition("}")[2] == name and element.text:
                return element.text
        return None

    def _parse_fault(self, root: ElementTree.Element, body: str) -> dict[str, Any]:
        """Describe a failed login in the shape the OAuth flows return

        :param root: The root of the parsed response
        :param body: The raw response body, used when the fault carries no \
        recognizable detail
        :return: The error and its description
        """
        return {
            "error": self._find_value(root, "exceptionCode") or "unknown_error",
            "error_description": (
                self._find_value(root, "exceptionMessage")
                or self._find_value(root, "faultstring")
                or body
            ),
        }

    def _parse_login_result(self, root: ElementTree.Element) -> dict[str, Any]:
        """Translate a successful login response into token attributes

        The response names no identity URL, so one is assembled from the
        ``organizationId`` of the login result's ``userInfo`` and its
        ``userId``. That is where Salesforce's reference client takes the
        ``tenantid`` call metadata from, and the assembled URL has the shape
        :py:meth:`~AuthenticatorBase.get_tenant_id` expects, so the base
        class derives the org ID without a special case.

        Both segments are required: an identity URL missing either one no
        longer has the org ID second from last, and would silently yield the
        wrong tenant rather than a usable error.

        :param root: The root of the parsed response
        :return: The session ID, the URL of the org's instance and the \
        identity URL
        :raise AuthenticationError: If the response carries no session, or \
        nothing to derive the org ID from
        """
        session_id = self._find_value(root, "sessionId")
        server_url = self._find_value(root, "serverUrl")
        organization_id = self._find_value(root, "organizationId")
        user_id = self._find_value(root, "userId")
        if session_id is None or server_url is None:
            self._clear_credentials()
            raise AuthenticationError(
                "Authentication failed", "The login response carries no session"
            )
        if organization_id is None or user_id is None:
            self._clear_credentials()
            raise AuthenticationError(
                "Authentication failed",
                "The login response carries no organization id and user id, "
                "which every Pub/Sub API call needs for its tenantid metadata",
            )

        parts = urlsplit(server_url)
        return {
            "access_token": session_id,
            # The SOAP response names no token type. The Pub/Sub API takes a
            # session ID in place of an access token, so it is sent the way
            # an access token is.
            "token_type": "Bearer",
            "instance_url": f"{parts.scheme}://{parts.netloc}",
            "id": (
                f"https://{self.domain}.salesforce.com/id/{organization_id}/{user_id}"
            ),
        }

    async def _authenticate(self) -> tuple[int, dict[str, Any]]:
        headers = {
            "Content-Type": "text/xml; charset=UTF-8",
            "SOAPAction": "login",
        }
        async with ClientSession() as session:
            response = await session.post(
                self._token_url,
                data=self._create_envelope().encode(),
                headers=headers,
            )
            body = await response.text()

        try:
            root = ElementTree.fromstring(body)
        except ElementTree.ParseError:
            self._clear_credentials()
            raise AuthenticationError(
                "Authentication failed", "The login response is not valid XML"
            ) from None

        if response.status != HTTPStatus.OK:
            return response.status, self._parse_fault(root, body)
        return response.status, self._parse_login_result(root)
