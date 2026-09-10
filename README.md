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

### Publishing

```python
result = await client.publish("/event/Your_Platform_Event__e", [{"Field__c": "value"}])
```

## Migration from `aiosfstream`

- **Authentication:** `PasswordAuthenticator`, `RefreshTokenAuthenticator` and
  `ClientCredentialsAuthenticator` take the same arguments as before.
- **Client:** replace `SalesforceStreamingClient` with
  `SalesforcePubSubClient`, which takes an authenticator rather than
  credentials directly.
- **Subscription:** instead of `await client.subscribe(channel)` followed by
  iterating the client, iterate the `client.subscribe(topic_name)` generator.
  There is no `unsubscribe`; stop iterating instead.
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

```bash
python -m venv venv
venv/bin/python -m pip install -e ".[dev]"
venv/bin/python -m pytest
```

## License
MIT
