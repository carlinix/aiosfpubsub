Advanced usage
==============

.. py:currentmodule:: simple_salesforce_pubsub

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

    from simple_salesforce_pubsub import ReplayOption, SalesforcePubSubClient

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

    from simple_salesforce_pubsub import ReplayMarkerStorage


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

    from simple_salesforce_pubsub import ReplayMarkerStoragePolicy

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

Re-authentication
-----------------

When the server rejects the access token, the authenticator runs again and the
subscription is re-established with the fresh token, without the consumer's
loop noticing.

A :meth:`~SalesforcePubSubClient.subscribe` subscription resumes from the
stored marker, or, when the storage holds none, from the position it
originally started at. That last part matters:
:obj:`ConstantReplayId` never stores anything, and
:obj:`~ReplayMarkerStoragePolicy.MANUAL` stores nothing until you commit, so
recomputing the position from scratch would restart from the default replay
option and drop everything published in between.

.. note::

    One case cannot be recovered. Under
    :obj:`~ReplayMarkerStoragePolicy.MANUAL` with
    :obj:`~ReplayOption.NEW_EVENTS`, if the very first response already
    carries events and none of them is committed, no position precedes them,
    so the subscription restarts from :obj:`~ReplayOption.NEW_EVENTS`. Commit
    as you go, or start from a stored marker, if that matters to you.

The number of consecutive attempts is bounded by ``auth_retries``, and the
count resets whenever a re-established subscription delivers an event. Routine
token expiry therefore never exhausts it, while credentials that have been
revoked fail quickly instead of hammering the token endpoint.

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
