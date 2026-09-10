# Simple Salesforce Pub/Sub API Client

A modern Python client for the [Salesforce Pub/Sub API](https://developer.salesforce.com/docs/platform/pub-sub-api/overview),
built on gRPC and `asyncio`. It is the successor to
[`aiosfstream`](https://github.com/carlinix/aiosfstream), which targeted the
older Salesforce Streaming API (CometD).

## Features

- Built on the official Salesforce gRPC Pub/Sub API.
- Fully asynchronous (`grpc.aio` and `aiohttp`).
- Transparent fetching and caching of Avro schemas, decoding with `fastavro`.
- Flow control: the event budget is replenished as events are consumed, so a
  subscription applies backpressure instead of stalling.
- Automatic re-authentication, resuming from the stored replay position.
- Two replay strategies: client-side replay marker storage, or Salesforce's
  own managed event subscriptions.
- Replay fallback for replay ids that aged out of the retention window.
- Change Data Capture header bitmaps expanded into field names.
- Streaming publish, and cancellable subscriptions.
- Authenticators matching `aiosfstream` for easy migration, including the
  OAuth 2.0 Client Credentials flow.

## Installation

```bash
pip install simple-salesforce-pubsub
```

Requires Python 3.11 or newer.

## Usage

```python
import asyncio

from simple_salesforce_pubsub import PasswordAuthenticator, SalesforcePubSubClient


async def main():
    auth = PasswordAuthenticator(
        consumer_key="YOUR_CONSUMER_KEY",
        consumer_secret="YOUR_CONSUMER_SECRET",
        username="YOUR_USERNAME",
        password="YOUR_PASSWORD",
        sandbox=False,
    )

    async with SalesforcePubSubClient(auth) as client:
        async for event in client.subscribe("/event/Your_Platform_Event__e"):
            print(event["id"], event["payload"])


if __name__ == "__main__":
    asyncio.run(main())
```

Each event is a mapping with `replay_id`, `id`, `schema_id`, `headers` and the
Avro-decoded `payload`.

### Replay: client-side storage

Pass a `ReplayOption`, any `MutableMapping[str, bytes]`, or a custom
`ReplayMarkerStorage` as the `replay` argument. A mapping lets a restarted
process resume where it stopped:

```python
import shelve

from simple_salesforce_pubsub import ReplayOption, SalesforcePubSubClient

with shelve.open("replay_markers") as markers:
    client = SalesforcePubSubClient(auth, replay=markers)
```

```python
# start from the beginning of the retention window instead of from now
client = SalesforcePubSubClient(auth, replay=ReplayOption.ALL_EVENTS)
```

By default the replay marker advances as soon as an event is consumed. To
advance it only after your own processing succeeded, use the manual policy:

```python
from simple_salesforce_pubsub import ReplayMarkerStoragePolicy

client = SalesforcePubSubClient(
    auth,
    replay=markers,
    replay_storage_policy=ReplayMarkerStoragePolicy.MANUAL,
)

async for event in client.subscribe(topic):
    await handle(event)
    await client.commit_replay(topic, event["replay_id"])
```

### Replay: managed event subscriptions

Salesforce can track the replay position for you, using a Managed Event
Subscription configured in the org. No client-side storage is involved:

```python
subscription = client.managed_subscribe(developer_name="My_Subscription")

async for event in subscription:
    await handle(event)
    await subscription.commit(event["replay_id"])  # only under MANUAL
```

### Replay fallback

A stored replay id eventually falls outside the event retention window, and
the server then rejects the subscription. Give a `replay_fallback` to have the
unusable position discarded and the subscription retried from a replay option
instead of raising:

```python
client = SalesforcePubSubClient(
    auth, replay=markers, replay_fallback=ReplayOption.ALL_EVENTS
)
```

The Pub/Sub API has no error code for this condition, so it is recognised from
the gRPC status. Override `SalesforcePubSubClient.is_replay_id_error()` if the
server wording changes.

### Change Data Capture

A CDC event reports the fields that changed as bitmaps over the event schema,
not as names. Pass `expand_change_event_header=True` to get names instead:

```python
client = SalesforcePubSubClient(auth, expand_change_event_header=True)

async for event in client.subscribe("/data/AccountChangeEvent"):
    print(event["payload"]["ChangeEventHeader"]["changedFields"])
    # ["Name", "BillingAddress.Street"]
```

It is off by default because it rewrites the decoded payload, and it is safe
to leave on for a client subscribed to platform events too. The underlying
`expand_change_event_header()`, `expand_bitmap_fields()` and `expand_bitmap()`
are exported for use on payloads decoded elsewhere.

### Reconnection

A subscription outlives the stream carrying it. It re-establishes itself when
the server rejects the access token and when the server closes the stream — a
`Subscribe` stream is closed if the event budget stays exhausted for about a
minute — so the `async for` loop ends only when you stop the subscription or
close the client, not because the connection did.

A stream that delivered events reconnects at once; one that did not is retried
with exponential backoff and gives up after a bounded number of consecutive
attempts, so a permanently broken subscription raises instead of looping:

```python
client = SalesforcePubSubClient(
    auth, reconnect_retries=10, retry_backoff=0.5, retry_backoff_max=30.0
)
```

### Stopping a subscription

```python
client.unsubscribe("/event/Your_Platform_Event__e")
```

The `async for` loop consuming that topic ends normally. A managed
subscription stops the same way, through `subscription.cancel()` or
`client.unsubscribe(subscription.name)`. Closing the client stops every active
subscription, and `client.subscriptions` lists them.

### Publishing

```python
response = await client.publish(
    "/event/Your_Platform_Event__e", [{"Field__c": "value"}]
)
```

A publish request can half succeed, with one result per record, so a rejected
record raises `PublishError`. The whole response is kept on the exception —
that is where the accepted records' replay ids are. Pass
`raise_on_error=False` to inspect the response yourself.

To publish repeatedly over a single stream, pass an asynchronous iterable of
record batches. Rejected records do not raise here, since tearing down a
long-lived stream over one bad batch defeats its purpose; inspect each
response, or pass one to `client.raise_for_results()`:

```python
async def batches():
    while True:
        yield [await next_record()]


async for response in client.publish_stream(topic, batches()):
    print(response.results)
```

## Migration from `aiosfstream`

- **Authentication:** `PasswordAuthenticator`, `RefreshTokenAuthenticator` and
  `ClientCredentialsAuthenticator` take the same arguments as before.
- **Client:** replace `SalesforceStreamingClient` with
  `SalesforcePubSubClient`, which takes an authenticator rather than
  credentials directly.
- **Subscription:** instead of `await client.subscribe(channel)` followed by
  iterating the client, iterate the `client.subscribe(topic_name)` generator.
  `unsubscribe(topic_name)` still exists and ends that iteration, but the
  Pub/Sub API opens one stream per topic rather than multiplexing every
  channel over a single connection.
- **Replay:** `ReplayOption`, `MappingStorage`, `ConstantReplayId` and
  `ReplayMarkerStoragePolicy` keep their names. Replay ids are now opaque
  `bytes` rather than integers, and `ReplayMarker` is gone — nothing needs the
  message creation date any more, so markers are plain replay ids.
  `DefaultMappingStorage` is also gone: pass `MappingStorage(mapping,
  default_option=...)` instead.
- **Topic names:** Change Data Capture (`/data/AccountChangeEvent`) and
  Platform Events (`/event/My_Event__e`).
- **Exceptions:** the hierarchy is rooted at `PubSubException` and no longer
  wraps CometD errors.

## Development

The project is managed with `uv`:

```bash
uv sync --all-groups
uv run ruff check . && uv run ruff format --check .
uv run coverage run -m pytest
uv run coverage report
uv run sphinx-build -b html -W docs/source docs/build/html
```

## Documentation

```bash
uv run sphinx-build -b html -W docs/source docs/build/html
```

## License

MIT. `auth.py` and `replay.py` are derived from
[`aiosfstream`](https://github.com/carlinix/aiosfstream), originally by
Róbert Márki, whose copyright notice is retained in `LICENSE.txt`.
