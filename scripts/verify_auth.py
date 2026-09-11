#!/usr/bin/env python3
"""Check an aiosfpubsub authenticator against a real org, stage by stage.

A token being issued is not the same as the Pub/Sub API accepting it: every RPC
also carries the org ID as ``tenantid`` call metadata, and each flow derives it
differently (the OAuth flows from the identity URL, the SOAP flow from the login
result). The stages are separate so a failure names what broke:

1. build the authenticator from the environment; nothing is contacted
2. authenticate, then derive ``instance_url`` and ``tenant_id``
3. call ``GetTopic`` and ``GetSchema`` over gRPC; authenticated, consumes nothing
4. optionally subscribe and listen for events

Usage:
    SF_AUTH_FLOW=client_credentials SF_CONSUMER_KEY=... SF_CONSUMER_SECRET=... \\
    SF_DOMAIN=mycompany.my SF_TOPIC=/event/Example__e \\
    uv run python scripts/verify_auth.py

Flows and the variables each one reads:
    client_credentials  SF_CONSUMER_KEY, SF_CONSUMER_SECRET, SF_DOMAIN
    password            SF_CONSUMER_KEY, SF_CONSUMER_SECRET, SF_USERNAME,
                        SF_PASSWORD, SF_SANDBOX
    soap                SF_USERNAME, SF_PASSWORD, SF_SECURITY_TOKEN, SF_DOMAIN,
                        SF_SANDBOX
    jwt                 SF_CONSUMER_KEY, SF_USERNAME, SF_PRIVATE_KEY_PATH,
                        SF_SANDBOX

Always required: SF_TOPIC. Optional: SF_LISTEN_SECONDS (default 30; 0 skips
the subscription) and SF_ENDPOINT (default api.pubsub.salesforce.com:7443).
"""

import asyncio
import os
import sys

import grpc

from aiosfpubsub import (
    AuthenticationError,
    AuthenticatorBase,
    ClientCredentialsAuthenticator,
    ClientError,
    JWTBearerAuthenticator,
    PasswordAuthenticator,
    SalesforcePubSubClient,
    SOAPAuthenticator,
)
from aiosfpubsub.client import DEFAULT_ENDPOINT

REQUIRED = {
    "client_credentials": ("SF_CONSUMER_KEY", "SF_CONSUMER_SECRET", "SF_DOMAIN"),
    "password": ("SF_CONSUMER_KEY", "SF_CONSUMER_SECRET", "SF_USERNAME", "SF_PASSWORD"),
    "soap": ("SF_USERNAME", "SF_PASSWORD"),
    "jwt": ("SF_CONSUMER_KEY", "SF_USERNAME", "SF_PRIVATE_KEY_PATH"),
}
#: Every variable each flow reads, required or not, in display order. Printing
#: only these keeps a variable exported for another flow out of the report.
READS = {
    "client_credentials": REQUIRED["client_credentials"],
    "password": (*REQUIRED["password"], "SF_SANDBOX"),
    "soap": (*REQUIRED["soap"], "SF_SECURITY_TOKEN", "SF_DOMAIN", "SF_SANDBOX"),
    "jwt": (*REQUIRED["jwt"], "SF_SANDBOX"),
}
SECRET_MARKERS = ("KEY", "SECRET", "PASSWORD", "TOKEN")


def mask(value: str | None, keep: int = 6) -> str:
    """Show only the tail of a secret, never the whole thing."""
    if not value:
        return "<empty>"
    if len(value) <= keep:
        return "<short>"
    return f"...{value[-keep:]} (len={len(value)})"


def stage(number: int, title: str) -> None:
    print(f"\n{'=' * 68}\n[{number}] {title}\n{'=' * 68}")


def fail(message: str, meaning: str) -> None:
    print(f"\n  FAILED: {message}")
    print(f"  What this means: {meaning}")


def is_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes"}


def build_authenticator(flow: str) -> AuthenticatorBase:
    """Build the authenticator for *flow* from the environment."""
    env = os.environ
    if flow == "client_credentials":
        return ClientCredentialsAuthenticator(
            consumer_key=env["SF_CONSUMER_KEY"],
            consumer_secret=env["SF_CONSUMER_SECRET"],
            domain=env["SF_DOMAIN"],
        )
    if flow == "password":
        return PasswordAuthenticator(
            consumer_key=env["SF_CONSUMER_KEY"],
            consumer_secret=env["SF_CONSUMER_SECRET"],
            username=env["SF_USERNAME"],
            password=env["SF_PASSWORD"],
            sandbox=is_true("SF_SANDBOX"),
        )
    if flow == "soap":
        return SOAPAuthenticator(
            username=env["SF_USERNAME"],
            password=env["SF_PASSWORD"],
            security_token=env.get("SF_SECURITY_TOKEN", ""),
            domain=env.get("SF_DOMAIN") or None,
            sandbox=is_true("SF_SANDBOX"),
        )
    # JWTBearerAuthenticator reads the key file here, in synchronous code,
    # rather than inside the event loop.
    return JWTBearerAuthenticator(
        consumer_key=env["SF_CONSUMER_KEY"],
        username=env["SF_USERNAME"],
        private_key_path=env["SF_PRIVATE_KEY_PATH"],
        sandbox=is_true("SF_SANDBOX"),
    )


def rpc_status(error: BaseException) -> str:
    """Describe *error* by its gRPC status code when one is available."""
    cause = error.__cause__
    if isinstance(cause, grpc.aio.AioRpcError):
        return f"{cause.code().name}: {cause.details()}"
    return f"{type(error).__name__}: {error}"


async def authenticate(authenticator: AuthenticatorBase) -> bool:
    stage(2, "Authenticate and derive the call metadata")
    print(f"  POST {authenticator._token_url}")
    try:
        await authenticator.authenticate()
    except AuthenticationError as exc:
        fail(
            str(exc),
            "The token request was rejected; the Pub/Sub API was never reached. "
            "Check the credentials, that the flow is enabled on the app, and "
            "for client_credentials that the app has a Run As user.",
        )
        return False

    print("  token issued    : OK")
    print(f"  token_type      : {authenticator.token_type}")
    print(f"  access_token    : {mask(authenticator.access_token)}")
    print(f"  instance_url    : {authenticator.instance_url}")
    print(f"  identity url    : {authenticator.id}")
    print(f"  tenant_id       : {authenticator.tenant_id}")
    if not authenticator.tenant_id:
        fail(
            "no tenant_id could be derived",
            "Every Pub/Sub RPC needs the org ID as tenantid metadata, and the "
            "token response carried no usable identity URL.",
        )
        return False

    keys = ", ".join(key for key, _ in authenticator.get_grpc_metadata())
    print(f"  call metadata   : {keys}")
    return True


async def listen_for_events(
    client: SalesforcePubSubClient, topic: str, seconds: float
) -> int:
    stage(4, f"Subscribe and listen {seconds:g}s")
    print("  (silence is normal - it only means no event fired)\n")
    received = 0
    try:
        async with asyncio.timeout(seconds):
            async for event in client.subscribe(topic):
                received += 1
                fields = ", ".join(sorted(event["payload"])[:6])
                print(f"  event {received}: id={event['id']} fields: {fields}")
    except TimeoutError:
        pass
    return received


async def check(
    authenticator: AuthenticatorBase, topic: str, endpoint: str, seconds: float
) -> int:
    if not await authenticate(authenticator):
        return 1

    stage(3, f"GetTopic and GetSchema over gRPC at {endpoint}")
    received = None
    # open() authenticates once more before opening the channel, which is what
    # every real client does, so this stage exercises the library's own path.
    client = SalesforcePubSubClient(authenticator, endpoint=endpoint)
    try:
        async with client:
            info = await client.get_topic_info(topic)
            print("  GetTopic        : OK  <- the Pub/Sub API accepted the metadata")
            print(f"  topic           : {info.topic_name}")
            print(f"  can_subscribe   : {info.can_subscribe}")
            print(f"  can_publish     : {info.can_publish}")
            schema = await client.get_schema(info.schema_id)
            print(f"  GetSchema       : OK  ({schema.get('name')})")
            if seconds > 0:
                received = await listen_for_events(client, topic, seconds)
    except ClientError as exc:
        fail(
            rpc_status(exc),
            "The token was issued but the gRPC call was not accepted. "
            "UNAUTHENTICATED: the API rejected accesstoken/instanceurl/tenantid. "
            "PERMISSION_DENIED: the user cannot read this topic. NOT_FOUND: the "
            "topic name is wrong. UNAVAILABLE: the network blocks gRPC to the "
            "endpoint, commonly port 7443 behind a corporate proxy.",
        )
        return 1
    except AuthenticationError as exc:
        fail(str(exc), "Re-authentication failed while the client was open.")
        return 1

    stage(5, "Result")
    print(f"  PASS - {type(authenticator).__name__} works with the Pub/Sub API.")
    if received is not None:
        print(f"  events received : {received}")
    return 0


def main() -> int:
    stage(1, "Configuration (nothing is contacted)")
    flow = os.getenv("SF_AUTH_FLOW", "client_credentials")
    if flow not in REQUIRED:
        fail(f"unknown SF_AUTH_FLOW {flow!r}", f"Use one of: {', '.join(REQUIRED)}.")
        return 2
    if missing := [n for n in (*REQUIRED[flow], "SF_TOPIC") if not os.getenv(n)]:
        fail(f"missing env vars: {', '.join(missing)}", "Set them and re-run.")
        return 2

    topic = os.environ["SF_TOPIC"]
    endpoint = os.getenv("SF_ENDPOINT", DEFAULT_ENDPOINT)
    seconds = float(os.getenv("SF_LISTEN_SECONDS", "30"))

    print(f"  flow            : {flow}")
    for name in READS[flow]:
        if (value := os.getenv(name)) is None:
            continue
        shown = mask(value) if any(m in name for m in SECRET_MARKERS) else value
        print(f"  {name:<16}: {shown}")
    print(f"  topic           : {topic}")
    print(f"  endpoint        : {endpoint}")

    try:
        authenticator = build_authenticator(flow)
    except (ValueError, ImportError, OSError) as exc:
        fail(
            f"{type(exc).__name__}: {exc}",
            "The authenticator rejected its arguments before contacting anything.",
        )
        return 2
    print(f"  authenticator   : {type(authenticator).__name__}")

    return asyncio.run(check(authenticator, topic, endpoint, seconds))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
