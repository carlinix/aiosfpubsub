from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aiohttp.client_exceptions import ClientConnectionError

from aiosfpubsub.auth import (
    SANDBOX_TOKEN_URL,
    TOKEN_URL,
    AuthenticatorBase,
    ClientCredentialsAuthenticator,
    PasswordAuthenticator,
    RefreshTokenAuthenticator,
)
from aiosfpubsub.exceptions import AuthenticationError

TOKEN_RESPONSE = {
    "access_token": "token",
    "token_type": "Bearer",
    "instance_url": "https://example.my.salesforce.com",
    "id": "https://login.salesforce.com/id/00D000000000000EAA/005000000000000AAA",
}


class AuthenticatorStub(AuthenticatorBase):
    def __init__(self, result=(HTTPStatus.OK, TOKEN_RESPONSE), **kwargs):
        super().__init__(**kwargs)
        self.result = result

    async def _authenticate(self):
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def session_returning(status, payload):
    """Return a patcher for a ClientSession posting once and answering with
    *status* and *payload*"""
    response = MagicMock()
    response.status = status
    response.json = AsyncMock(return_value=payload)
    session = MagicMock()
    session.post = AsyncMock(return_value=response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return patch("aiosfpubsub.auth.ClientSession", return_value=session), (session)


def test_token_url_depends_on_sandbox():
    assert AuthenticatorStub()._token_url == TOKEN_URL
    assert AuthenticatorStub(sandbox=True)._token_url == SANDBOX_TOKEN_URL


@pytest.mark.parametrize(
    ("identity_url", "expected"),
    [
        ("https://login.salesforce.com/id/00D000EAA/005000AAA", "00D000EAA"),
        ("https://login.salesforce.com/id/00D000EAA/005000AAA/", "00D000EAA"),
        ("nope", None),
        ("", None),
        (None, None),
    ],
)
def test_get_tenant_id(identity_url, expected):
    assert AuthenticatorBase.get_tenant_id(identity_url) == expected


@pytest.mark.asyncio
async def test_authenticate_stores_the_token_data():
    authenticator = AuthenticatorStub()

    await authenticator.authenticate()

    assert authenticator.access_token == "token"
    assert authenticator.token_type == "Bearer"
    assert authenticator.instance_url == "https://example.my.salesforce.com"
    assert authenticator.tenant_id == "00D000000000000EAA"


@pytest.mark.asyncio
async def test_authenticate_clears_the_token_data_on_rejection():
    authenticator = AuthenticatorStub()
    await authenticator.authenticate()
    authenticator.result = (HTTPStatus.BAD_REQUEST, {"error": "invalid_grant"})

    with pytest.raises(AuthenticationError):
        await authenticator.authenticate()

    assert authenticator.access_token is None
    assert authenticator.instance_url is None
    assert authenticator.tenant_id is None


@pytest.mark.asyncio
async def test_authenticate_translates_network_failures():
    authenticator = AuthenticatorStub(result=ClientConnectionError("down"))

    with pytest.raises(AuthenticationError, match="Network request failed"):
        await authenticator.authenticate()


@pytest.mark.asyncio
async def test_get_grpc_metadata():
    authenticator = AuthenticatorStub()
    await authenticator.authenticate()

    assert authenticator.get_grpc_metadata() == (
        ("accesstoken", "token"),
        ("instanceurl", "https://example.my.salesforce.com"),
        ("tenantid", "00D000000000000EAA"),
    )


def test_get_grpc_metadata_before_authenticating():
    with pytest.raises(AuthenticationError, match="without authenticating first"):
        AuthenticatorStub().get_grpc_metadata()


@pytest.mark.asyncio
async def test_password_authenticator_sends_the_password_grant():
    authenticator = PasswordAuthenticator(
        consumer_key="key",
        consumer_secret="secret",
        username="user",
        password="pass",
    )
    patcher, session = session_returning(HTTPStatus.OK, TOKEN_RESPONSE)
    with patcher:
        await authenticator.authenticate()

    url, kwargs = session.post.await_args.args[0], session.post.await_args.kwargs
    assert url == TOKEN_URL
    assert kwargs["data"] == {
        "grant_type": "password",
        "client_id": "key",
        "client_secret": "secret",
        "username": "user",
        "password": "pass",
    }


@pytest.mark.asyncio
async def test_refresh_token_authenticator_sends_the_refresh_grant():
    authenticator = RefreshTokenAuthenticator(
        consumer_key="key", consumer_secret="secret", refresh_token="refresh"
    )
    patcher, session = session_returning(HTTPStatus.OK, TOKEN_RESPONSE)
    with patcher:
        await authenticator.authenticate()

    assert session.post.await_args.kwargs["data"]["grant_type"] == "refresh_token"
    assert session.post.await_args.kwargs["data"]["refresh_token"] == "refresh"


@pytest.mark.asyncio
async def test_client_credentials_authenticator_uses_the_my_domain_host():
    authenticator = ClientCredentialsAuthenticator(
        consumer_key="key", consumer_secret="secret", domain="mycompany.my"
    )
    patcher, session = session_returning(HTTPStatus.OK, TOKEN_RESPONSE)
    with patcher:
        await authenticator.authenticate()

    assert (
        session.post.await_args.args[0]
        == "https://mycompany.my.salesforce.com/services/oauth2/token"
    )
    assert session.post.await_args.kwargs["data"] == {
        "grant_type": "client_credentials",
        "client_id": "key",
        "client_secret": "secret",
    }


@pytest.mark.parametrize(
    "domain",
    [
        "",
        "   ",
        "https://mycompany.my.salesforce.com",
        "mycompany.my.salesforce.com",
        "login",
        "test",
    ],
)
def test_client_credentials_authenticator_rejects_bad_domains(domain):
    with pytest.raises(ValueError):
        ClientCredentialsAuthenticator("key", "secret", domain)


def test_client_credentials_authenticator_normalises_the_domain():
    authenticator = ClientCredentialsAuthenticator("key", "secret", " mycompany.my/ ")

    assert authenticator.domain == "mycompany.my"


@pytest.mark.parametrize(
    "authenticator",
    [
        PasswordAuthenticator("key", "secret", "user", "pass"),
        RefreshTokenAuthenticator("key", "secret", "refresh"),
        ClientCredentialsAuthenticator("key", "secret", "mycompany.my"),
    ],
)
def test_repr_names_the_class(authenticator):
    assert repr(authenticator).startswith(f"{type(authenticator).__name__}(")


def test_repr_truncates_long_secrets():
    secret = "s" * 200
    authenticator = PasswordAuthenticator("key", secret, "user", "pass")

    assert secret not in repr(authenticator)
    assert "..." in repr(authenticator)
