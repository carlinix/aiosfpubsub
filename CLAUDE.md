# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow

**Never commit to `main`.** Branch first, always — including for a one-line
docs fix. `main` tracks `origin/main` and only moves through a merged pull
request. If work has already landed on `main` locally, move it: branch at the
current tip, then `git reset --hard origin/main`.

Name branches `<type>/<subject>` with the same types the commit convention
uses (`feat/jwt-bearer-authenticator`, `docs/workflow-conventions`). One
branch per concern; when two features touch the same files, stack the second
on the first and say so in its PR description rather than mixing them.

**Everything in this repository is written in English** — code, comments,
docstrings, documentation, tests, commit messages, branch names, and pull
request titles and descriptions. This holds regardless of the language the
work is being discussed in.

## Environment & Commands

The project is managed with `uv`, which owns `.venv`.

```bash
uv sync --all-groups                              # create/refresh .venv
uv run coverage run -m pytest                     # tests with branch coverage
uv run coverage report
uv run pytest -k replenish                        # single test by name pattern
uv run pytest tests/test_client.py::test_publish_wraps_rpc_errors
uv run ruff check . && uv run ruff format .       # lint and format
uv run sphinx-build -b html -W docs/source docs/build/html
uv build                                          # wheel + sdist
```

Dependency groups mirror the sibling project: `test`, `lint`, `docs`, `build`,
plus `proto` for `grpcio-tools`. The only `[project.optional-dependencies]`
entry is `jwt` (`pyjwt[crypto]`, for `JWTBearerAuthenticator`); there is no
`dev` extra, so `pip install -e ".[dev]"` no longer exists.

`asyncio_mode = "strict"`, so every async test needs an explicit
`@pytest.mark.asyncio`. The suite is at 100% branch coverage of the
hand-written modules; the generated `pubsub_api_pb2*.py` are excluded from
ruff and coverage, and simply never referenced by the docs. CI enforces
`ruff format --check` and a
`-W` (warnings-as-errors) docs build, so run both before committing.

## Architecture

### Name

The package was renamed from `simple-salesforce-pubsub` on 2026-09-10. That name implied a relationship with [`simple-salesforce`](https://github.com/simple-salesforce/simple-salesforce), an unrelated REST/Bulk client by a different author, which this project neither imports nor depends on. `aiosfpubsub` follows the `aiosf*` convention of the sibling project. See "Remaining gaps" for the PyPI situation.

### Purpose and reference implementation

This package is the successor to the user's own `aiosfstream` (CometD Streaming API client), retargeted at Salesforce's gRPC **Pub/Sub API** (https://developer.salesforce.com/docs/platform/pub-sub-api/overview). The reference implementation is [`carlinix/aiosfstream`](https://github.com/carlinix/aiosfstream) — read it before designing any new surface here. It is usually checked out alongside this repository.

Names and argument shapes mirror `aiosfstream` wherever the semantics survive the transport change (`PasswordAuthenticator`, `ClientCredentialsAuthenticator`, `ReplayOption`, `MappingStorage`, `ReplayMarkerStoragePolicy`), so migration is mostly mechanical. **The client surface is still not API-compatible**: `aiosfstream.Client` is async-iterable with `subscribe`/`unsubscribe` multiplexed over one CometD connection, whereas `SalesforcePubSubClient.subscribe()` returns one async generator per topic, each backed by its own gRPC stream, and there is no `unsubscribe` — you stop iterating.

### Conventions come from the sibling project

`aiosfstream` is the house style, and this project now mirrors it: `uv_build` backend with `module-root = ""` (the package sits at the repo root, not under `src/`), `uv.lock`, dependency groups, ruff config, pytest config, branch coverage, `py.typed`, `docs/source` layout with `.readthedocs.yaml`, and a `ci.yml` of the same shape.

`LICENSE.txt` carries **two** copyright lines. `auth.py` and `replay.py` are derived from aiosfstream, originally by Róbert Márki, so his notice is retained alongside the user's. Don't drop it.

The one workflow deliberately **not** copied is `release.yml`: the sibling publishes to PyPI and a GCP Artifact Registry under its own package name and secrets, which needs the user's decision on target registry and credentials.

### Modules

- `auth.py` — `AuthenticatorBase` performs the token exchange via `aiohttp`; most subclasses supply only the form body in `_authenticate()`. The base derives `tenant_id` (the org ID) from the `id` URL, and `get_grpc_metadata()` turns the credentials into the `accesstoken` / `instanceurl` / `tenantid` call metadata every Pub/Sub RPC requires. Two subclasses override `_token_url`, for unrelated reasons: `ClientCredentialsAuthenticator` because that flow is only served from an org's My Domain host (and it validates `domain` up front rather than letting a malformed URL surface as a 404), and `SOAPAuthenticator` because `login()` lives at `/services/Soap/u/{version}`. `JWTBearerAuthenticator` signs an RS256 assertion with PyJWT, which is an optional dependency — the import is guarded and the constructor raises a message naming the `jwt` extra, so the docs build and every non-JWT user work without it.

  **`SOAPAuthenticator` is the one that doesn't fit the shape.** It is not OAuth: it posts an XML envelope and parses XML back, and it rejects a malformed-but-`200` response from inside `_authenticate()` — which is why `_clear_credentials()` was extracted from `authenticate()`, since the base only clears on a non-OK status. Most importantly, the SOAP login response carries **no identity URL**, so there is nothing for the base to derive `tenant_id` from. One is synthesized from the login result's `organizationId` and `userId` — which is where Salesforce's own reference client (`python/InventoryAppExample/PubSub.py` in `forcedotcom/pub-sub-api`) takes `tenantid` from — in the `.../id/<org>/<user>` shape `get_tenant_id()` expects. **Both segments are required**: with either one missing the org ID is no longer second from last, so defaulting one away would resolve the wrong tenant silently instead of raising. Elements are matched by local name (`_find_value`) rather than by position as the reference client does, since the namespaces differ between a login result and a fault. `SOAP_API_VERSION` is pinned to 59.0 because `login()` is already unavailable in 65.0 and later, and Salesforce retires it from 31.0 through 64.0 in Summer '27 — the whole flow has a deadline, so don't steer anyone to it.
- `replay.py` — client-side replay position tracking, ported from `aiosfstream.replay`. **The port is not mechanical**: the Streaming API had an ordered integer replay id plus a message creation date, which let it discard replayed messages by comparing dates. Pub/Sub replay ids are opaque `bytes` — not ordered, not comparable — and no creation date exists outside the Avro payload, so `ReplayMarker`, `get_message_date` and the staleness check have no counterpart. Markers are stored unconditionally; the `Subscribe` stream itself guarantees ordering. `get_fetch_position()` is the seam the client uses: a stored marker becomes `(CUSTOM, marker)`, otherwise `(default_option, b"")`. `DefaultMappingStorage` is gone — its role collapses into `MappingStorage(mapping, default_option=...)`. `SalesforcePubSubClient.connect` is an alias of `open()`, kept for the original 0.1.0 surface.
- `client.py` — `SalesforcePubSubClient` owns the `grpc.aio` channel and the schema cache, plus the two subscription paths described below.
- `cdc.py` — expands the `ChangeEventHeader` bitmaps of a Change Data Capture event into field names. The bitmaps are **LSB-first over the event record's own field list**, hex encoded, with compound fields written `parentPosition-childBitmap`. This mirrors `python/util/ChangeEventHeaderUtility.py` in `forcedotcom/pub-sub-api`, but without its `bitstring` and `avro` dependencies: `int(bitmap, 16) >> index & 1` gives the same bit order (verified against that implementation), and named-type references are resolved through `fastavro`'s `__named_schemas`, since fastavro writes the second use of a named type as its name alone. `SalesforcePubSubClient(expand_change_event_header=True)` applies it per event; off by default because it rewrites the decoded payload.
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

`num_requested` is a credit, not a batch size, and the credits are cumulative: the proto states that requesting more before the server has delivered the outstanding amount makes it *add* to the outstanding total. Replenishing exactly at zero matches Salesforce's own reference Node client. Note the timing rule that comes with it: while `pending_num_requested > 0` the server keeps the stream alive with an empty `FetchResponse` within 270 seconds, but once it reaches zero the client has ~60 seconds to send the next `FetchRequest` or the server closes the stream. A consumer slower than that will lose its stream (see "Remaining gaps").

`subscribe()` wraps `_subscribe_once()` in a loop that handles three ways a stream can end, and returns only on cancellation:

- **`UNAUTHENTICATED`** → `_AuthenticationExpired`: the authenticator runs again and the stream reopens with **fresh call metadata**, the only supported way to present a new token. `FetchRequest.auth_refresh` looks like it exists for this and does not — the proto marks it "For internal Salesforce use only" in all three messages that carry it. Do not wire it up.
- **A clean end** (the `else` branch): the server closed the stream, which is never a normal end for a long-lived `Subscribe`. It reopens from the resume position.
- **`CANCELLED`** → `_SubscriptionCancelled`: `unsubscribe()` or `close()`. This is the only path that returns.

Both retrying branches share the same shape: the consecutive-failure counter resets whenever the stream delivered an event, so routine reconnection never exhausts it while a permanently broken subscription raises rather than looping forever (the failure mode of CAMEL-21740 in the Camel connector). `auth_retries` and `reconnect_retries` bound them separately. A stream that delivered events reopens immediately; an unproductive one waits `backoff_delay(attempt)` — exponential doubling from `retry_backoff` to `retry_backoff_max`, with full jitter so clients recovering from one outage don't retry in lockstep. `backoff_delay()` is overridable, which is how the tests make retries deterministic and instant.

`ManagedSubscription.__aiter__()` mirrors all of this, and additionally requeues unacknowledged commits on every restart. The retry count is bounded by `auth_retries` and reset whenever a resumed subscription delivers an event, so routine hourly expiries never exhaust it but revoked credentials fail fast instead of hammering the token endpoint.

`_ResumePosition` decides where the re-established stream starts, and exists because the replay storage is legitimately empty in two cases — `ConstantReplayId` never stores, and `MANUAL` stores nothing until the consumer commits. Recomputing `get_fetch_position()` there would restart from the default option and drop everything published in between. The resolution order is: the stored marker, then an in-memory position advanced under `AUTOMATIC` (which covers `ConstantReplayId`), then the position the subscription originally started from. A `NEW_EVENTS` subscription additionally *anchors* itself: the first keepalive received before any event names a concrete replay id, which replaces the `LATEST` preset so a resume is exact rather than "from now" again.

Two residual limitations, each asserted in a named test so a change to them is deliberate (`test_manual_policy_without_a_marker_cannot_anchor_before_an_event`, `test_a_stream_closed_before_any_response_cannot_anchor`). Both need `NEW_EVENTS` with nothing stored: a first response that already carries uncommitted events under `MANUAL`, and a stream closed before producing any response. Neither leaves a position preceding what was missed — nothing in the protocol exposes a "position before this event" — so the resume falls back to `LATEST`. A single keepalive is enough to avoid both, which covers the common idle case.

### Regenerating the protobuf stubs

The `.proto` is **not** vendored here; it must come from Salesforce's upstream `forcedotcom/pub-sub-api` repository (the older `developerforce` name still redirects).

```bash
uv sync --group proto
uv run python -m grpc_tools.protoc -I<proto_dir> \
  --python_out=aiosfpubsub --grpc_python_out=aiosfpubsub \
  pubsub_api.proto
```

`protoc` emits `import pubsub_api_pb2 as pubsub__api__pb2` at the top of `pubsub_api_pb2_grpc.py`, which breaks inside a package. The checked-in file has been patched to `from . import pubsub_api_pb2 as pubsub__api__pb2` — **re-apply that patch after every regeneration.**

### replay_fallback is a heuristic

`replay_fallback` mirrors `aiosfstream`: when the server rejects the replay id the subscription started from — typically because it aged out of the **72 hour** retention window — the marker is discarded and the subscription is retried once from the fallback option. The proto's `ErrorCode` covers only `{UNKNOWN, PUBLISH, COMMIT}`, both publish-side, so there is no protocol error code for this. Salesforce reports it as an `INVALID_ARGUMENT` status carrying its own code in the **trailing metadata**, under `error-code`: `sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted`. `is_replay_id_error()` checks that trailer first and falls back to matching "replay" in the status description, and is a `staticmethod` so it can be overridden. **If replay fallback stops triggering, look there first.**

The managed path needs no equivalent: when a committed replay id is invalid, retrying `ManagedSubscribe` restarts from the `errorRecoveryReplay` field configured on the org's `ManagedEventSubscription` record.

### Commit acknowledgements are not one-to-one

The proto is explicit that N `CommitReplayRequest`s can be batched into a single `CommitReplayResponse` naming only the **last** one. `_record_commit_response()` therefore clears every pending commit up to and including the acknowledged id — insertion order in `pending_commits` is submission order. Clearing only the named id leaks the rest, growing the mapping without bound and resending them on every restart. Two cases clear nothing: a response with an `error` set (`ErrorCode.COMMIT` is documented as unrecoverable, so dropping the position would be worse), and an acknowledgement for an id this subscription never submitted.

### Publish failures are per record

`Publish` answers with a `PublishResult` per submitted record, so a batch can half succeed. `publish()` raises `PublishError` on any rejected record, and the exception keeps the whole `PublishResponse` — the accepted records' replay ids live there and would otherwise be lost. `raise_on_error=False` returns the response instead.

`publish_stream()` deliberately does **not** raise: tearing down a stream meant to stay open over one bad batch defeats its purpose. It exposes `raise_for_results()` so a caller can opt into the same error per batch. Keep that asymmetry — it is a decision, not an oversight.

## Remaining gaps

- **No `release.yml`.** Publishing needs the user's call on target registry and credentials; see "Conventions" above.
- **`ci.yml` triggers on `main` where the sibling uses `develop`.** The remote now exists — `github.com/carlinix/aiosfpubsub`, created 2026-09-10, which is what `project.urls` points at — and CI runs there green on all nine jobs, so the branch name is the one piece of the sibling's shape this repo deliberately does not copy. Whether to adopt `develop` is still undecided.
- **The distribution name is taken on PyPI.** `aiosfpubsub` belongs to `bensnyde/aiosfpubsub`, last released 2024-06-29 (0.0.6, 5 releases in one week, 0 stars, Unlicense). **`uv publish` will be rejected.** The plan is a voluntary transfer request to the author; the formal PEP 541 route is a poor fit, since this project is not a fork of that one, alternative names are available, and it has no users yet — three of the five things a requester must demonstrate. If the transfer does not come through, the fallback names verified free on 2026-09-10 are `aiosfeventbus` and `sfpubsub`. Do not publish under a substitute name without asking.
- **Managed subscriptions are mock-tested only.** They need a Managed Event Subscription configured in a real org, so nothing here has run against Salesforce.
- **`publish_stream()` does not enforce the 70 second liveness rule.** The proto requires a publish request with at least one event every 70 seconds to hold the stream open; a slow `batches` iterable silently loses it.
- **`ProducerEvent.id` is never set.** The proto allows a user-provided id, and `PublishResult.correlation_key` is what correlates a result back to its record. `publish()` sends records only, so results can only be matched positionally.
