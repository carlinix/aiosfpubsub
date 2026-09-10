# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment & Commands

A local `venv/` (Python 3.14) holds the installed dependencies; prefer `venv/bin/python` over a bare `python`.

```bash
venv/bin/python -m pip install -e ".[dev]"   # editable install with test extras
venv/bin/python -m pytest                    # run the suite
venv/bin/python -m pytest -k replenish       # single test by name pattern
venv/bin/python -m pytest tests/test_client.py::test_publish_wraps_rpc_errors
venv/bin/python -m ruff check .              # lint
venv/bin/python -m coverage run -m pytest    # tests with branch coverage
venv/bin/python -m coverage report
```

`asyncio_mode = "strict"`, so every async test needs an explicit `@pytest.mark.asyncio`. The suite is at 100% branch coverage of the hand-written modules; the generated `pubsub_api_pb2*.py` are excluded from both ruff and coverage. `ruff format` is deliberately **not** used — the sibling project lints without formatting, and `E` already enforces the line length.

## Architecture

### Purpose and reference implementation

This package is the successor to the user's own `aiosfstream` (CometD Streaming API client), retargeted at Salesforce's gRPC **Pub/Sub API** (https://developer.salesforce.com/docs/platform/pub-sub-api/overview). The reference implementation lives at **`/home/ricardo-sperandio/Projects/aiosfstream`** — read it before designing any new surface here.

Names and argument shapes mirror `aiosfstream` wherever the semantics survive the transport change (`PasswordAuthenticator`, `ClientCredentialsAuthenticator`, `ReplayOption`, `MappingStorage`, `ReplayMarkerStoragePolicy`), so migration is mostly mechanical. **The client surface is still not API-compatible**: `aiosfstream.Client` is async-iterable with `subscribe`/`unsubscribe` multiplexed over one CometD connection, whereas `SalesforcePubSubClient.subscribe()` returns one async generator per topic, each backed by its own gRPC stream, and there is no `unsubscribe` — you stop iterating.

### Conventions come from the sibling project

`/home/ricardo-sperandio/Projects/aiosfstream/pyproject.toml` is the house style. Adopted here: ruff (`line-length = 88`, `select = [ANN, ASYNC, B, C4, E, F, I, SIM, UP]`, `ignore = [ANN401]`, `target-version = "py311"`), pytest (`--strict-config --strict-markers -ra`, `asyncio_mode = "strict"`), branch coverage, and the `py.typed` marker. Still on the sibling and not here: the `uv_build` backend with `uv.lock` (this project still uses setuptools), Sphinx docs, and the GitHub Actions workflows.

### Modules

- `auth.py` — `AuthenticatorBase` performs the OAuth2 token exchange via `aiohttp`; subclasses supply only the form body in `_authenticate()`. The base derives `tenant_id` (the org ID) from the OAuth `id` URL, and `get_grpc_metadata()` turns the credentials into the `accesstoken` / `instanceurl` / `tenantid` call metadata every Pub/Sub RPC requires. `ClientCredentialsAuthenticator` overrides `_token_url` because that flow is only served from an org's My Domain host, and validates `domain` up front rather than letting a malformed URL surface as a 404.
- `replay.py` — client-side replay position tracking, ported from `aiosfstream.replay`. **The port is not mechanical**: the Streaming API had an ordered integer replay id plus a message creation date, which let it discard replayed messages by comparing dates. Pub/Sub replay ids are opaque `bytes` — not ordered, not comparable — and no creation date exists outside the Avro payload, so `ReplayMarker`, `get_message_date` and the staleness check have no counterpart. Markers are stored unconditionally; the `Subscribe` stream itself guarantees ordering. `get_fetch_position()` is the seam the client uses: a stored marker becomes `(CUSTOM, marker)`, otherwise `(default_option, b"")`. `DefaultMappingStorage` is gone — its role collapses into `MappingStorage(mapping, default_option=...)`. `SalesforcePubSubClient.connect` is an alias of `open()`, kept for the original 0.1.0 surface.
- `client.py` — `SalesforcePubSubClient` owns the `grpc.aio` channel and the schema cache, plus the two subscription paths described below.
- `pubsub_api_pb2.py` / `pubsub_api_pb2_grpc.py` — generated code, do not hand-edit except as noted below.

### Subscription lifecycle

Both `subscribe()` and `ManagedSubscription.__aiter__()` register a `_StreamSlot` in `client._streams` — keyed by topic name for the former, by `subscription_id or developer_name` for the latter. `unsubscribe(name)` cancels the slot's call, which surfaces as `CANCELLED` and ends the generator normally for its consumer; `ManagedSubscription.cancel()` is the same thing under its own name; `close()` cancels every registered slot. Registering twice under one name raises `ClientInvalidOperation`, since two streams sharing a name would fight over the same replay position. `client.subscriptions` exposes the active set.

The slot indirection is not decoration. Entries are released **by identity** (`if self._streams.get(name) is slot`), because an abandoned generator runs its cleanup only when it is closed — potentially after the same name has been subscribed to again. Releasing by key alone would deregister the newer subscription and leave it uncancellable. A slot is also reserved before its call exists, so `_StreamSlot.cancel()` tolerates a missing call.

This is not the multiplexing `aiosfstream` had, and it can't be: CometD carried every channel over one connection, whereas the Pub/Sub API gives one bidirectional stream per `Subscribe` call. Each topic costs a stream.

### The two replay strategies

Both are supported, and they are deliberately separate entry points because managed subscriptions are addressed by `subscription_id` / `developer_name` rather than by topic name:

- `subscribe(topic_name)` — position tracked **client side** in `client.replay_storage`. Configure with the `replay` constructor argument (a `ReplayOption`, any `MutableMapping[str, bytes]`, or a custom `ReplayMarkerStorage`).
- `managed_subscribe(...)` — returns a `ManagedSubscription`, with the position tracked **server side** by Salesforce via `CommitReplayRequest`. Requires a Managed Event Subscription configured in the org, so it can only be exercised against mocks in tests.

`ReplayMarkerStoragePolicy` governs both: `AUTOMATIC` advances the position as each event is consumed, `MANUAL` waits for `commit_replay()` (client side) or `ManagedSubscription.commit()` (managed). The two cost very different things — client-side `AUTOMATIC` is a local mapping write, managed `AUTOMATIC` queues a `CommitReplayRequest` per event onto the request stream. That asymmetry is inherent to the managed path; batching commits would change delivery semantics, so don't "optimise" it away without deciding that deliberately. One asymmetry worth preserving: a `FetchResponse` keepalive carries no events but a fresh `latest_replay_id`. `AUTOMATIC` advances on it, so an idle subscription doesn't re-read the retention window on reconnect; `MANUAL` must not, since nothing was consumed.

### Flow control and re-authentication

`_subscribe_once()` drives the request stream from an `asyncio.Queue`. The initial `FetchRequest` carries the replay position; afterwards, whenever a response reports `pending_num_requested <= 0`, another `FetchRequest` is enqueued. Because the generator only resumes when the consumer asks for the next event, this is what applies backpressure. Do not "simplify" this back into a single-request generator — the stream stalls after `num_requested` events.

`subscribe()` wraps `_subscribe_once()` in a loop: a gRPC `UNAUTHENTICATED` becomes the internal `_AuthenticationExpired`, the authenticator runs again, and the subscription is re-established from the stored replay marker with the fresh token in `FetchRequest.auth_refresh`. The retry count is bounded by `auth_retries` and reset whenever a resumed subscription delivers an event, so routine hourly expiries never exhaust it but revoked credentials fail fast instead of hammering the token endpoint.

`_ResumePosition` decides where the re-established stream starts, and exists because the replay storage is legitimately empty in two cases — `ConstantReplayId` never stores, and `MANUAL` stores nothing until the consumer commits. Recomputing `get_fetch_position()` there would restart from the default option and drop everything published in between. The resolution order is: the stored marker, then an in-memory position advanced under `AUTOMATIC` (which covers `ConstantReplayId`), then the position the subscription originally started from. A `NEW_EVENTS` subscription additionally *anchors* itself: the first keepalive received before any event names a concrete replay id, which replaces the `LATEST` preset so a resume is exact rather than "from now" again.

One residual limitation, asserted in `test_manual_policy_without_a_marker_cannot_anchor_before_an_event`: under `MANUAL` + `NEW_EVENTS`, if the very first response already carries events and none is committed, no position precedes them, so the resume falls back to `LATEST`. Nothing in the protocol exposes a "position before this event".

### Regenerating the protobuf stubs

The `.proto` is **not** vendored here; it must come from Salesforce's upstream `developerforce/pub-sub-api` repository.

```bash
venv/bin/python -m grpc_tools.protoc -I<proto_dir> \
  --python_out=simple_salesforce_pubsub --grpc_python_out=simple_salesforce_pubsub \
  pubsub_api.proto
```

`protoc` emits `import pubsub_api_pb2 as pubsub__api__pb2` at the top of `pubsub_api_pb2_grpc.py`, which breaks inside a package. The checked-in file has been patched to `from . import pubsub_api_pb2 as pubsub__api__pb2` — **re-apply that patch after every regeneration.**

### replay_fallback is a heuristic

`replay_fallback` mirrors `aiosfstream`: when the server rejects the replay id the subscription started from — typically because it aged out of the retention window — the marker is discarded and the subscription is retried once from the fallback option. Unlike the Streaming API there is no error code for this; the proto's `ErrorCode` covers only `{UNKNOWN, PUBLISH, COMMIT}`, both publish-side. So `is_replay_id_error()` matches on the gRPC status instead (`INVALID_ARGUMENT` whose details mention "replay") and is a `staticmethod` precisely so it can be overridden when Salesforce changes the wording. **If replay fallback stops triggering, look there first.**

## Remaining gaps

- **No Sphinx docs and no `uv_build` migration.** The sibling has both, plus `uv.lock` and GitHub Actions workflows.
- **`ManagedSubscription` has no re-auth path.** `subscribe()` re-establishes itself on `UNAUTHENTICATED`; the managed path propagates it as a `ClientError`. The server holds the position, so a caller can simply iterate again, but the asymmetry is real.
- **`publish()` and `publish_stream()` don't inspect `PublishResult.error`.** A per-record failure is reported in the response, not raised.
