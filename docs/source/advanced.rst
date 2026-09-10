Advanced usage
==============

.. py:currentmodule:: aiosfpubsub

Replay
------

Every event carries a replay id naming its position in the topic. Salesforce
retains events for a limited window, so a client that stops and restarts can
pick up where it left off by replaying from a stored position rather than from
the present moment.

Unlike the Streaming API, a Pub/Sub API replay id is an opaque :class:`bytes`
value. It is not an integer, it cannot be compared or ordered, and there is no
message creation date outside the Avro payload. Treat it as a token to hand
back to Salesforce.

There are two ways to track the position, and they are separate entry points.

Client side storage
~~~~~~~~~~~~~~~~~~~

:meth:`~SalesforcePubSubClient.subscribe` tracks the position in the client's
:obj:`ReplayMarkerStorage`, selected with the ``replay`` argument. It accepts
a :obj:`ReplayOption`:

.. code-block:: python

    from aiosfpubsub import ReplayOption, SalesforcePubSubClient

    # only events published from now on, the default
    client = SalesforcePubSubClient(auth, replay=ReplayOption.NEW_EVENTS)

    # everything still inside the retention window first
    client = SalesforcePubSubClient(auth, replay=ReplayOption.ALL_EVENTS)

any object supporting the :class:`~collections.abc.MutableMapping` protocol,
which is what lets a restarted process resume:

.. code-block:: python

    import shelve

    with shelve.open("replay_markers") as markers:
        client = SalesforcePubSubClient(auth, replay=markers)

or a custom :obj:`ReplayMarkerStorage` implementation, for a database or any
other backing store:

.. code-block:: python

    from aiosfpubsub import ReplayMarkerStorage


    class DatabaseStorage(ReplayMarkerStorage):
        async def get_replay_marker(self, topic_name):
            return await db.fetch_marker(topic_name)

        async def set_replay_marker(self, topic_name, replay_id):
            await db.store_marker(topic_name, replay_id)

        async def clear_replay_marker(self, topic_name):
            await db.delete_marker(topic_name)

Managed event subscriptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:meth:`~SalesforcePubSubClient.managed_subscribe` hands the position to
Salesforce, which stores it against a `managed event subscription
<managed_subscriptions_>`_ configured in the org. The client's replay storage
is not consulted at all:

.. code-block:: python

    subscription = client.managed_subscribe(developer_name="My_Subscription")

    async for event in subscription:
        await handle(event)

Managed subscriptions are addressed by ``subscription_id`` or
``developer_name`` rather than by topic name, which is why this is a separate
method rather than an argument to
:meth:`~SalesforcePubSubClient.subscribe`.

Storage policies
~~~~~~~~~~~~~~~~

:obj:`ReplayMarkerStoragePolicy` governs both strategies.

Under :obj:`~ReplayMarkerStoragePolicy.AUTOMATIC`, the default, the position
advances as soon as an event is consumed. The downside is that an event counts
as consumed even if your processing of it then fails.

Under :obj:`~ReplayMarkerStoragePolicy.MANUAL` nothing advances until you say
so, which is what you want when a failure has to lead to redelivery:

.. code-block:: python

    from aiosfpubsub import ReplayMarkerStoragePolicy

    client = SalesforcePubSubClient(
        auth,
        replay=markers,
        replay_storage_policy=ReplayMarkerStoragePolicy.MANUAL,
    )

    async for event in client.subscribe(topic):
        await handle(event)
        await client.commit_replay(topic, event["replay_id"])

The managed equivalent is :meth:`ManagedSubscription.commit`:

.. code-block:: python

    async for event in subscription:
        await handle(event)
        await subscription.commit(event["replay_id"])

A managed commit is only acknowledged once the server answers. Until then it
stays in :obj:`ManagedSubscription.pending_commits`, and it is sent again if
the stream has to be re-established, so it is not lost with the stream it was
queued on. The acknowledgements arrive in
:obj:`ManagedSubscription.commit_responses`, keyed by the request id
:meth:`~ManagedSubscription.commit` returned.

.. note::

    Acknowledgements are not one to one. The server may batch several commits
    and answer once, naming only the last of them, which acknowledges every
    commit submitted before it as well. A commit whose response carries an
    error is left pending, since ``ErrorCode.COMMIT`` marks an unrecoverable
    commit failure.

Replay fallback
~~~~~~~~~~~~~~~

A stored replay id eventually falls outside the 72 hour retention window, and
the server then rejects the subscription. Give a ``replay_fallback`` to have the
unusable position discarded and the subscription retried once from a replay
option instead of raising:

.. code-block:: python

    client = SalesforcePubSubClient(
        auth, replay=markers, replay_fallback=ReplayOption.ALL_EVENTS
    )

It can also be given per subscription::

    client.subscribe(topic, replay_fallback=ReplayOption.ALL_EVENTS)

.. warning::

    The Pub/Sub API's ``ErrorCode`` enum covers only publish and commit
    failures, so there is no protocol code for this condition. Salesforce
    reports it as an ``INVALID_ARGUMENT`` status carrying
    ``sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted`` in
    the ``error-code`` trailing metadata, which
    :meth:`~SalesforcePubSubClient.is_replay_id_error` checks, falling back to
    the status description. It is a static method so that you can override it
    in a subclass if Salesforce changes either. If replay fallback stops
    triggering, look there first.

Managed subscriptions need no fallback: if a committed replay id is invalid,
retrying restarts the subscription from the ``errorRecoveryReplay`` value
configured on the ``ManagedEventSubscription`` record in your org.

Change Data Capture
-------------------

A Change Data Capture event does not name the fields that changed. Its
``ChangeEventHeader`` reports them as bitmaps over the event schema's own
field list, so a decoded payload looks like this:

.. code-block:: python

    {"ChangeEventHeader": {"changedFields": ["0x02", "2-0x01"], ...}, ...}

``"0x02"`` is a hexadecimal bitmap of the top level fields, least significant
bit first, so bit *n* stands for the *n*-th field of the event record.
``"2-0x01"`` is a compound field: the number is the position of the parent
field, and the bitmap that follows covers the fields of the nested record.

Pass ``expand_change_event_header=True`` to have the client replace all three
bitmap lists with the names they stand for:

.. code-block:: python

    client = SalesforcePubSubClient(auth, expand_change_event_header=True)

    async for event in client.subscribe("/data/AccountChangeEvent"):
        print(event["payload"]["ChangeEventHeader"]["changedFields"])
        # ["Name", "BillingAddress.Street"]

It is off by default because it rewrites the decoded payload, and it is safe
to leave on for a client that also subscribes to platform events: a payload
without a ``ChangeEventHeader`` is left untouched.

The functions behind it are public, for expanding a payload you decoded some
other way:

.. code-block:: python

    from aiosfpubsub import expand_change_event_header

    schema = await client.get_schema(event["schema_id"])
    expand_change_event_header(schema, event["payload"])

:func:`~aiosfpubsub.expand_bitmap_fields` expands one bitmap list
and :func:`~aiosfpubsub.expand_bitmap` a single bitmap against a
given field list. All three raise :obj:`~exceptions.SchemaError` if a bitmap
cannot be expanded against the schema, rather than returning a partial answer.

Flow control
------------

A subscription asks the server for ``num_requested`` events at a time and
replenishes that budget as events are consumed. Because the generator only
resumes when its consumer asks for the next event, a slow consumer slows the
subscription down rather than filling memory:

.. code-block:: python

    client = SalesforcePubSubClient(auth, num_requested=100)

    # or, for one subscription
    client.subscribe(topic, num_requested=100)

Reconnection
------------

A subscription outlives the stream carrying it. Both
:meth:`~SalesforcePubSubClient.subscribe` and
:meth:`~SalesforcePubSubClient.managed_subscribe` re-establish themselves in
two situations, without the consumer's loop noticing:

- The server rejects the access token. The authenticator runs again and the
  stream reopens with the new token.
- The server closes the stream. A ``Subscribe`` stream is long lived but not
  permanent — it is closed if the event budget stays exhausted for about a
  minute — and Salesforce's guidance is to call ``Subscribe`` again.

Iteration therefore ends only when you stop the subscription or close the
client. It does not end because the connection did.

A stream that delivered events reconnects at once. One that did not is retried
with an exponential backoff, doubling from ``retry_backoff`` up to
``retry_backoff_max`` with jitter, and gives up after ``auth_retries`` or
``reconnect_retries`` consecutive attempts:

.. code-block:: python

    client = SalesforcePubSubClient(
        auth,
        reconnect_retries=10,
        retry_backoff=0.5,
        retry_backoff_max=30.0,
    )

Both counters reset as soon as a re-established stream delivers an event, so a
healthy long-lived subscription reconnects indefinitely while a permanently
broken one raises instead of retrying forever. Override
:meth:`~SalesforcePubSubClient.backoff_delay` for a different schedule.

Where a subscription resumes
----------------------------

A :meth:`~SalesforcePubSubClient.subscribe` subscription resumes from the
stored marker, or, when the storage holds none, from the position it
originally started at. That last part matters:
:obj:`ConstantReplayId` never stores anything, and
:obj:`~ReplayMarkerStoragePolicy.MANUAL` stores nothing until you commit, so
recomputing the position from scratch would restart from the default replay
option and drop everything published in between.

A keepalive is enough to anchor a subscription, so a stream that sat idle and
was then closed resumes exactly where it was rather than skipping whatever was
published during the backoff.

.. note::

    Two cases cannot be recovered, both requiring
    :obj:`~ReplayOption.NEW_EVENTS` with nothing stored: a stream whose very
    first response already carries events, none of which is committed under
    :obj:`~ReplayMarkerStoragePolicy.MANUAL`, and a stream closed before it
    produced any response at all. Neither leaves a position that precedes what
    was missed, so the subscription restarts from
    :obj:`~ReplayOption.NEW_EVENTS`. Commit as you go, use a storing replay
    storage, or start from :obj:`~ReplayOption.ALL_EVENTS`, if that matters to
    you.

Stopping a subscription
-----------------------

:meth:`~SalesforcePubSubClient.unsubscribe` cancels the stream, which ends the
consumer's loop normally:

.. code-block:: python

    client.unsubscribe("/event/Your_Event__e")

A managed subscription stops the same way, through
:meth:`ManagedSubscription.cancel` or by passing its
:obj:`~ManagedSubscription.name`. Closing the client stops everything, and
:obj:`~SalesforcePubSubClient.subscriptions` lists what is active.

.. note::

    The Pub/Sub API opens one bidirectional stream per subscription. Unlike
    CometD, which carried every channel over a single connection, each topic
    costs a stream here. Subscribing twice under one name is rejected with
    :obj:`~exceptions.ClientInvalidOperation`, since two streams would
    otherwise fight over the same replay position.

Publishing over a stream
------------------------

:meth:`~SalesforcePubSubClient.publish` pays a round trip per call.
:meth:`~SalesforcePubSubClient.publish_stream` keeps one stream open and
yields the server's response for each batch:

.. code-block:: python

    async def batches():
        while True:
            yield [await next_record()]


    async for response in client.publish_stream(topic, batches()):
        print(response.results)

.. warning::

    The server closes a publish stream unless it receives a request with at
    least one event every 70 seconds. Keeping the stream alive is the caller's
    responsibility: yield often enough, or use
    :meth:`~SalesforcePubSubClient.publish` for sporadic traffic.

Rejected records do not raise here: tearing the stream down over one bad batch
would defeat the point of keeping it open. Inspect each response, or pass one
to :meth:`~SalesforcePubSubClient.raise_for_results` to get the same
:obj:`PublishError` :meth:`~SalesforcePubSubClient.publish` would have raised.

.. include:: global.rst
