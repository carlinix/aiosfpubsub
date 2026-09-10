Migration from aiosfstream
==========================

.. py:currentmodule:: aiosfpubsub

aiosfstream_ speaks the Salesforce Streaming API over CometD_; this package
speaks the Pub/Sub API over gRPC. The two protocols differ enough that the
client surface could not be kept compatible, but the names and arguments that
survived the change were kept deliberately.

Authenticators
--------------

Unchanged. :obj:`PasswordAuthenticator`,
:obj:`RefreshTokenAuthenticator`, :obj:`ClientCredentialsAuthenticator`,
:obj:`JWTBearerAuthenticator` and :obj:`SOAPAuthenticator` take the same
arguments as before.

Client
------

Replace ``SalesforceStreamingClient`` with :obj:`SalesforcePubSubClient`,
which takes an authenticator rather than credentials directly:

.. code-block:: python

    # aiosfstream
    client = SalesforceStreamingClient(
        consumer_key="...", consumer_secret="...", username="...", password="..."
    )

    # aiosfpubsub
    client = SalesforcePubSubClient(
        PasswordAuthenticator(
            consumer_key="...",
            consumer_secret="...",
            username="...",
            password="...",
        )
    )

Subscriptions
-------------

``aiosfstream.Client`` subscribed to a channel and was then iterated as a
whole. Here each subscription is its own asynchronous generator:

.. code-block:: python

    # aiosfstream
    await client.subscribe("/topic/Foo")
    async for message in client:
        ...

    # aiosfpubsub
    async for event in client.subscribe("/event/Foo__e"):
        ...

:meth:`~SalesforcePubSubClient.unsubscribe` still exists and ends that
iteration, but it no longer shares a connection with the other subscriptions:
the Pub/Sub API opens one stream per subscription.

Message shape
-------------

CometD messages were nested under ``data``. Pub/Sub events arrive already
Avro-decoded, as a flat mapping of ``replay_id``, ``id``, ``schema_id``,
``headers`` and ``payload``. See :doc:`quickstart`.

Topics
------

PushTopics and Generic Streaming have no Pub/Sub counterpart. What carries
over is Platform Events, now under ``/event/``, and Change Data Capture, now
under ``/data/``.

Replay
------

:obj:`ReplayOption`, :obj:`MappingStorage`,
:obj:`ConstantReplayId` and :obj:`ReplayMarkerStoragePolicy` keep their names
and their roles, with three differences:

- Replay ids are opaque :class:`bytes` rather than integers.
- ``ReplayMarker`` is gone. It paired a replay id with a message creation
  date so that replayed messages could be discarded by comparing dates;
  neither the ordering nor the date exists here, so a marker is now just a
  replay id.
- ``DefaultMappingStorage`` is gone. Pass
  :obj:`MappingStorage(mapping, default_option=...) <MappingStorage>`
  instead.

:obj:`ReplayMarkerStorage` implementations gain a third method,
:meth:`~ReplayMarkerStorage.clear_replay_marker`, which
``replay_fallback`` uses to discard a position the server rejected.

Exceptions
----------

The hierarchy is rooted at :obj:`~exceptions.PubSubException` and no longer
wraps CometD errors, so ``TransportError`` and ``ServerError`` have no
counterpart. :obj:`~exceptions.AuthenticationError`,
:obj:`~exceptions.ClientError`, :obj:`~exceptions.ClientInvalidOperation` and
:obj:`~exceptions.ReplayError` keep their names, and
:obj:`~exceptions.SchemaError` and :obj:`~exceptions.PublishError` are new.

Publishing
----------

``aiosfstream.Client.publish`` was a CometD operation Salesforce never
implemented for Platform Events, so the documentation pointed you at the REST
API. The Pub/Sub API publishes natively: see
:meth:`~SalesforcePubSubClient.publish` and
:meth:`~SalesforcePubSubClient.publish_stream`.

.. include:: global.rst
