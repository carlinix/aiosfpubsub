import reprlib
import time
from http import HTTPStatus
from unittest.mock import AsyncMock, MagicMock, patch
from xml.etree import ElementTree

import jwt
import pytest
from aiohttp.client_exceptions import ClientConnectionError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from aiosfpubsub.auth import (
    AUDIENCE_URL,
    JWT_ALGORITHM,
    JWT_BEARER_GRANT_TYPE,
    JWT_EXPIRATION,
    LOGIN_DOMAIN,
    SANDBOX_AUDIENCE_URL,
    SANDBOX_LOGIN_DOMAIN,
    SANDBOX_TOKEN_URL,
    SOAP_API_VERSION,
    TOKEN_URL,
    AuthenticatorBase,
    ClientCredentialsAuthenticator,
    JWTBearerAuthenticator,
    PasswordAuthenticator,
    RefreshTokenAuthenticator,
    SOAPAuthenticator,
)
from aiosfpubsub.exceptions import AuthenticationError

# Generated once: 2048 bit key generation is slow enough to notice when
# repeated for every test in this module.
PRIVATE_KEY_OBJECT = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PRIVATE_KEY = PRIVATE_KEY_OBJECT.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
).decode()
PUBLIC_KEY = (
    PRIVATE_KEY_OBJECT.public_key()
    .public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    .decode()
)

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
        JWTBearerAuthenticator("key", "user", private_key=PRIVATE_KEY),
        SOAPAuthenticator("user@acme.com", "pass"),
    ],
)
def test_repr_names_the_class(authenticator):
    assert repr(authenticator).startswith(f"{type(authenticator).__name__}(")


def test_repr_truncates_long_secrets():
    secret = "s" * 200
    authenticator = PasswordAuthenticator("key", secret, "user", "pass")

    assert secret not in repr(authenticator)
    assert "..." in repr(authenticator)


def decode_assertion(assertion, audience=AUDIENCE_URL):
    return jwt.decode(
        assertion, PUBLIC_KEY, algorithms=[JWT_ALGORITHM], audience=audience
    )


@pytest.fixture
def jwt_authenticator():
    return JWTBearerAuthenticator(
        consumer_key="key", username="user@example.com", private_key=PRIVATE_KEY
    )


def test_jwt_authenticator_reads_the_private_key_from_a_path(tmp_path):
    key_file = tmp_path / "server.key"
    # Written as bytes: write_text would translate the newlines on Windows,
    # and the key is read back verbatim with read_bytes.
    key_file.write_bytes(PRIVATE_KEY.encode())

    authenticator = JWTBearerAuthenticator(
        consumer_key="key", username="user@example.com", private_key_path=key_file
    )

    assert authenticator.private_key == PRIVATE_KEY.encode()


def test_jwt_authenticator_rejects_a_missing_key_path(tmp_path):
    """A bad key path must not surface as an authentication failure later"""
    with pytest.raises(OSError):
        JWTBearerAuthenticator(
            consumer_key="key",
            username="user@example.com",
            private_key_path=tmp_path / "absent.key",
        )


def test_jwt_authenticator_rejects_no_key():
    with pytest.raises(ValueError, match="exactly one of private_key"):
        JWTBearerAuthenticator(consumer_key="key", username="user@example.com")


def test_jwt_authenticator_rejects_both_keys(tmp_path):
    with pytest.raises(ValueError, match="exactly one of private_key"):
        JWTBearerAuthenticator(
            consumer_key="key",
            username="user@example.com",
            private_key=PRIVATE_KEY,
            private_key_path=tmp_path / "server.key",
        )


@pytest.mark.parametrize("expiration", [0, -1])
def test_jwt_authenticator_rejects_a_non_positive_expiration(expiration):
    with pytest.raises(ValueError, match="must be positive"):
        JWTBearerAuthenticator(
            consumer_key="key",
            username="user@example.com",
            private_key=PRIVATE_KEY,
            expiration=expiration,
        )


def test_jwt_authenticator_without_pyjwt_installed():
    with (
        patch("aiosfpubsub.auth.jwt", None),
        pytest.raises(ImportError, match=r"aiosfpubsub\[jwt\]"),
    ):
        JWTBearerAuthenticator(
            consumer_key="key", username="user@example.com", private_key=PRIVATE_KEY
        )


def test_jwt_authenticator_audience_follows_the_sandbox_flag():
    """The aud claim and the token endpoint must name the same org"""
    production = JWTBearerAuthenticator("key", "user", private_key=PRIVATE_KEY)
    sandbox = JWTBearerAuthenticator(
        "key", "user", private_key=PRIVATE_KEY, sandbox=True
    )

    assert (production.audience, production._token_url) == (AUDIENCE_URL, TOKEN_URL)
    assert (sandbox.audience, sandbox._token_url) == (
        SANDBOX_AUDIENCE_URL,
        SANDBOX_TOKEN_URL,
    )


def test_jwt_authenticator_accepts_an_audience_override():
    """Experience Cloud sites are addressed by their own URL"""
    authenticator = JWTBearerAuthenticator(
        "key",
        "user",
        private_key=PRIVATE_KEY,
        audience="https://site.force.com/customers",
    )

    assert authenticator.audience == "https://site.force.com/customers"


def test_jwt_authenticator_signs_the_assertion_claims(jwt_authenticator):
    before = int(time.time())

    claims = decode_assertion(jwt_authenticator._create_assertion())

    assert claims["iss"] == "key"
    assert claims["sub"] == "user@example.com"
    assert claims["aud"] == AUDIENCE_URL
    assert before + JWT_EXPIRATION <= claims["exp"] <= int(time.time()) + JWT_EXPIRATION


def test_jwt_authenticator_signs_with_the_private_key(jwt_authenticator):
    other_key = (
        rsa.generate_private_key(public_exponent=65537, key_size=2048)
        .public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )

    with pytest.raises(jwt.InvalidSignatureError):
        jwt.decode(
            jwt_authenticator._create_assertion(),
            other_key,
            algorithms=[JWT_ALGORITHM],
            audience=AUDIENCE_URL,
        )


def test_jwt_authenticator_honours_a_custom_expiration():
    authenticator = JWTBearerAuthenticator(
        "key", "user", private_key=PRIVATE_KEY, expiration=30
    )
    before = int(time.time())

    claims = decode_assertion(authenticator._create_assertion())

    assert before + 30 <= claims["exp"] <= int(time.time()) + 30


@pytest.mark.asyncio
async def test_jwt_authenticator_sends_the_jwt_bearer_grant(jwt_authenticator):
    patcher, session = session_returning(HTTPStatus.OK, TOKEN_RESPONSE)
    with patcher:
        await jwt_authenticator.authenticate()

    assert session.post.await_args.args[0] == TOKEN_URL
    data = session.post.await_args.kwargs["data"]
    assert set(data) == {"grant_type", "assertion"}
    assert data["grant_type"] == JWT_BEARER_GRANT_TYPE
    assert decode_assertion(data["assertion"])["iss"] == "key"


@pytest.mark.asyncio
async def test_jwt_authenticator_populates_the_grpc_metadata(jwt_authenticator):
    """The token response carries the identity URL every RPC's tenantid needs"""
    patcher, _ = session_returning(HTTPStatus.OK, TOKEN_RESPONSE)
    with patcher:
        await jwt_authenticator.authenticate()

    assert jwt_authenticator.get_grpc_metadata() == (
        ("accesstoken", "token"),
        ("instanceurl", "https://example.my.salesforce.com"),
        ("tenantid", "00D000000000000EAA"),
    )


@pytest.mark.asyncio
async def test_jwt_authenticator_signs_a_fresh_assertion_per_attempt(jwt_authenticator):
    """A re-authentication after an expired token must not replay the old one"""
    patcher, session = session_returning(HTTPStatus.OK, TOKEN_RESPONSE)
    with patcher:
        await jwt_authenticator.authenticate()
        first = session.post.await_args.kwargs["data"]["assertion"]
        jwt_authenticator.expiration += 1
        await jwt_authenticator.authenticate()
        second = session.post.await_args.kwargs["data"]["assertion"]

    assert first != second


def test_jwt_authenticator_repr_hides_the_private_key(jwt_authenticator):
    result = repr(jwt_authenticator)

    assert result == (
        "JWTBearerAuthenticator("
        f"consumer_key={reprlib.repr('key')}, "
        f"username={reprlib.repr('user@example.com')}, "
        f"audience={reprlib.repr(AUDIENCE_URL)})"
    )
    assert "PRIVATE KEY" not in result
    assert PRIVATE_KEY.splitlines()[1][:16] not in result


LOGIN_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns="urn:partner.soap.sforce.com"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Body>
    <loginResponse>
      <result>
        <metadataServerUrl>https://mycompany.my.salesforce.com/services/Soap/m/59.0/00D0EAA</metadataServerUrl>
        <passwordExpired>false</passwordExpired>
        <sandbox>false</sandbox>
        <serverUrl>https://mycompany.my.salesforce.com/services/Soap/u/59.0/00D0EAA</serverUrl>
        <sessionId>00D000000000000!AQ4AQExample</sessionId>
        <userId>005000000000000AAA</userId>
        <userInfo>
          <accessibilityMode>false</accessibilityMode>
          <organizationId>00D000000000000EAA</organizationId>
          <organizationName>Acme</organizationName>
          <userEmail>user@acme.com</userEmail>
        </userInfo>
      </result>
    </loginResponse>
  </soapenv:Body>
</soapenv:Envelope>"""

LOGIN_FAULT = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:sf="urn:fault.partner.soap.sforce.com"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <soapenv:Body>
    <soapenv:Fault>
      <faultcode>sf:INVALID_LOGIN</faultcode>
      <faultstring>INVALID_LOGIN: Invalid username, password,
        security token; or user locked out.</faultstring>
      <detail>
        <sf:LoginFault xsi:type="sf:LoginFault">
          <sf:exceptionCode>INVALID_LOGIN</sf:exceptionCode>
          <sf:exceptionMessage>Invalid username, password,
            security token; or user locked out.</sf:exceptionMessage>
        </sf:LoginFault>
      </detail>
    </soapenv:Fault>
  </soapenv:Body>
</soapenv:Envelope>"""


def soap_session_returning(body=LOGIN_RESPONSE, status=HTTPStatus.OK):
    """Return a patcher for a ClientSession answering a login request with
    *status* and the XML *body*"""
    response = MagicMock()
    response.status = status
    response.text = AsyncMock(return_value=body)
    session = MagicMock()
    session.post = AsyncMock(return_value=response)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    return patch("aiosfpubsub.auth.ClientSession", return_value=session), session


@pytest.fixture
def soap_authenticator():
    return SOAPAuthenticator(username="user@acme.com", password="password")


def test_soap_authenticator_defaults_to_the_login_host(soap_authenticator):
    assert soap_authenticator.domain == LOGIN_DOMAIN
    assert soap_authenticator._token_url == (
        f"https://login.salesforce.com/services/Soap/u/{SOAP_API_VERSION}"
    )


def test_soap_authenticator_sandbox_uses_the_test_host():
    authenticator = SOAPAuthenticator("user@acme.com.uat", "pass", sandbox=True)

    assert authenticator.domain == SANDBOX_LOGIN_DOMAIN
    assert authenticator._token_url == (
        f"https://test.salesforce.com/services/Soap/u/{SOAP_API_VERSION}"
    )


def test_soap_api_version_stays_below_the_login_cutoff():
    """login() is unavailable in SOAP API 65.0 and later"""
    assert float(SOAP_API_VERSION) < 65.0


def test_soap_authenticator_explicit_domain_wins_over_sandbox():
    authenticator = SOAPAuthenticator(
        "user@acme.com", "pass", domain=" mycompany.my/ ", sandbox=True
    )

    assert authenticator.domain == "mycompany.my"
    assert authenticator._token_url == (
        f"https://mycompany.my.salesforce.com/services/Soap/u/{SOAP_API_VERSION}"
    )


@pytest.mark.parametrize(
    ("domain", "message"),
    [
        ("", "must not be empty"),
        ("   ", "must not be empty"),
        ("/", "must not be empty"),
        ("https://mycompany.my.salesforce.com", "not a URL"),
        ("mycompany.my.salesforce.com", "salesforce.com"),
    ],
)
def test_soap_authenticator_rejects_bad_domains(domain, message):
    with pytest.raises(ValueError, match=message):
        SOAPAuthenticator("user@acme.com", "pass", domain=domain)


def test_soap_authenticator_appends_the_security_token(soap_authenticator):
    soap_authenticator.security_token = "XyZ123"

    envelope = soap_authenticator._create_envelope()

    assert "<n1:username>user@acme.com</n1:username>" in envelope
    assert "<n1:password>passwordXyZ123</n1:password>" in envelope


def test_soap_authenticator_escapes_the_credentials():
    """A password holding & or < would otherwise produce malformed XML"""
    authenticator = SOAPAuthenticator("a&b@acme.com", "p<a>ss&", security_token="t&t")

    envelope = authenticator._create_envelope()

    assert "<n1:username>a&amp;b@acme.com</n1:username>" in envelope
    assert "<n1:password>p&lt;a&gt;ss&amp;t&amp;t</n1:password>" in envelope
    ElementTree.fromstring(envelope)


@pytest.mark.asyncio
async def test_soap_authenticator_posts_the_envelope(soap_authenticator):
    patcher, session = soap_session_returning()
    with patcher:
        await soap_authenticator.authenticate()

    assert session.post.await_args.args[0] == soap_authenticator._token_url
    assert (
        session.post.await_args.kwargs["data"]
        == soap_authenticator._create_envelope().encode()
    )
    headers = session.post.await_args.kwargs["headers"]
    assert headers["SOAPAction"] == "login"
    assert headers["Content-Type"] == "text/xml; charset=UTF-8"


@pytest.mark.asyncio
async def test_soap_authenticator_populates_the_grpc_metadata(soap_authenticator):
    """The session ID stands in for an access token, and tenantid comes from
    the login result's organizationId, as in Salesforce's reference client"""
    patcher, _ = soap_session_returning()
    with patcher:
        await soap_authenticator.authenticate()

    assert soap_authenticator.token_type == "Bearer"
    assert soap_authenticator.get_grpc_metadata() == (
        ("accesstoken", "00D000000000000!AQ4AQExample"),
        ("instanceurl", "https://mycompany.my.salesforce.com"),
        ("tenantid", "00D000000000000EAA"),
    )


@pytest.mark.asyncio
async def test_soap_authenticator_instance_url_is_the_origin_of_server_url(
    soap_authenticator,
):
    """serverUrl names the SOAP endpoint; instanceurl wants only its origin"""
    patcher, _ = soap_session_returning()
    with patcher:
        await soap_authenticator.authenticate()

    assert soap_authenticator.instance_url == "https://mycompany.my.salesforce.com"


@pytest.mark.asyncio
async def test_soap_authenticator_translates_a_fault(soap_authenticator):
    patcher, _ = soap_session_returning(
        body=LOGIN_FAULT, status=HTTPStatus.INTERNAL_SERVER_ERROR
    )
    with patcher:
        status, data = await soap_authenticator._authenticate()

    assert status == HTTPStatus.INTERNAL_SERVER_ERROR
    assert data["error"] == "INVALID_LOGIN"
    assert "Invalid username" in data["error_description"]


@pytest.mark.asyncio
async def test_soap_authenticator_fault_clears_the_session(soap_authenticator):
    soap_authenticator.access_token = "stale"
    soap_authenticator.tenant_id = "00D000000000000EAA"
    patcher, _ = soap_session_returning(
        body=LOGIN_FAULT, status=HTTPStatus.INTERNAL_SERVER_ERROR
    )

    with patcher, pytest.raises(AuthenticationError, match="Authentication failed"):
        await soap_authenticator.authenticate()

    assert soap_authenticator.access_token is None
    assert soap_authenticator.tenant_id is None


@pytest.mark.asyncio
async def test_soap_authenticator_falls_back_to_the_faultstring(soap_authenticator):
    body = (
        "<soapenv:Envelope xmlns:soapenv="
        '"http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body>'
        "<soapenv:Fault><faultcode>soapenv:Client</faultcode>"
        "<faultstring>content type not allowed</faultstring>"
        "</soapenv:Fault></soapenv:Body></soapenv:Envelope>"
    )
    patcher, _ = soap_session_returning(
        body=body, status=HTTPStatus.INTERNAL_SERVER_ERROR
    )
    with patcher:
        _, data = await soap_authenticator._authenticate()

    assert data["error"] == "unknown_error"
    assert data["error_description"] == "content type not allowed"


@pytest.mark.asyncio
async def test_soap_authenticator_fault_without_any_detail(soap_authenticator):
    body = (
        "<soapenv:Envelope xmlns:soapenv="
        '"http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body>'
        "<soapenv:Fault/></soapenv:Body></soapenv:Envelope>"
    )
    patcher, _ = soap_session_returning(
        body=body, status=HTTPStatus.INTERNAL_SERVER_ERROR
    )
    with patcher:
        _, data = await soap_authenticator._authenticate()

    assert data["error"] == "unknown_error"
    assert data["error_description"] == body


@pytest.mark.asyncio
async def test_soap_authenticator_without_a_session_clears_and_raises(
    soap_authenticator,
):
    """A 200 that carries no sessionId must not leave a stale session behind"""
    soap_authenticator.access_token = "stale"
    body = (
        "<soapenv:Envelope xmlns:soapenv="
        '"http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body>'
        "<loginResponse><result/></loginResponse>"
        "</soapenv:Body></soapenv:Envelope>"
    )
    patcher, _ = soap_session_returning(body=body)

    with patcher, pytest.raises(AuthenticationError, match="carries no session"):
        await soap_authenticator.authenticate()

    assert soap_authenticator.access_token is None


@pytest.mark.parametrize(
    "missing",
    [
        "<organizationId>00D000000000000EAA</organizationId>",
        "<userId>005000000000000AAA</userId>",
    ],
)
@pytest.mark.asyncio
async def test_soap_authenticator_without_an_org_id_clears_and_raises(
    soap_authenticator, missing
):
    """Every Pub/Sub RPC needs tenantid, so a login it can't be derived from
    is useless here even though the Streaming API would have accepted it.
    Either segment missing leaves the org ID somewhere get_tenant_id would
    not look, so neither may be defaulted away."""
    soap_authenticator.access_token = "stale"
    patcher, _ = soap_session_returning(body=LOGIN_RESPONSE.replace(missing, ""))

    with patcher, pytest.raises(AuthenticationError, match="no organization id"):
        await soap_authenticator.authenticate()

    assert soap_authenticator.access_token is None


@pytest.mark.asyncio
async def test_soap_authenticator_rejects_a_non_xml_response(soap_authenticator):
    soap_authenticator.access_token = "stale"
    # An HTML error page from a proxy in front of the org: not well formed,
    # because of the unclosed <br>.
    patcher, _ = soap_session_returning(
        body="<html><body>502 Bad Gateway<br></body></html>"
    )

    with patcher, pytest.raises(AuthenticationError, match="not valid XML"):
        await soap_authenticator.authenticate()

    assert soap_authenticator.access_token is None


@pytest.mark.asyncio
async def test_soap_authenticator_translates_network_failures(soap_authenticator):
    with (
        patch("aiosfpubsub.auth.ClientSession", side_effect=ClientConnectionError()),
        pytest.raises(AuthenticationError, match="Network request failed"),
    ):
        await soap_authenticator.authenticate()


def test_soap_authenticator_repr_hides_the_password():
    authenticator = SOAPAuthenticator(
        "user@acme.com", "s3cret", security_token="XyZ123"
    )

    result = repr(authenticator)

    assert result == (
        f"SOAPAuthenticator(username={reprlib.repr('user@acme.com')}, "
        f"domain={reprlib.repr(LOGIN_DOMAIN)})"
    )
    assert "s3cret" not in result
    assert "XyZ123" not in result
