Salesforce Pub/Sub API client for asyncio
=========================================

:mod:`aiosfpubsub` is a Python client for the `Salesforce
Pub/Sub API <api_>`_, built on gRPC_ and asyncio_. It is the successor to
aiosfstream_, which targeted the older Salesforce Streaming API over CometD_.

The Pub/Sub API is a single gRPC interface for publishing and subscribing to
`Platform Events <PlatformEvents_>`_ and `Change Data Capture
<ChangeDataCapture_>`_ events. Event payloads are Avro encoded, and this
client fetches, caches and applies the schemas for you, so events arrive as
plain Python mappings.

Features
--------

- Built on the official Salesforce gRPC Pub/Sub API.
- Fully asynchronous, on :mod:`grpc.aio` and aiohttp_.
- Transparent fetching and caching of Avro schemas, decoding with fastavro_.
- Flow control: the event budget is replenished as events are consumed, so a
  subscription applies backpressure instead of stalling.
- Automatic re-authentication, resuming from the position the subscription
  was at.
- Two replay strategies: client side replay marker storage, or Salesforce's
  own managed event subscriptions.
- Replay fallback for replay ids that aged out of the retention window.
- Streaming publish, and cancellable subscriptions.
- Authenticators matching aiosfstream_ for an easy migration, including the
  OAuth 2.0 Client Credentials flow.

.. include:: global.rst
