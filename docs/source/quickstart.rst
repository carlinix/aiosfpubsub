Quickstart
==========

Subscribing
-----------

Create an authenticator, hand it to a
:obj:`~simple_salesforce_pubsub.SalesforcePubSubClient`, and iterate a
subscription:

.. code-block:: python

    import asyncio

    from simple_salesforce_pubsub import (
        PasswordAuthenticator,
        SalesforcePubSubClient,
    )


    async def main():
        auth = PasswordAuthenticator(
            consumer_key="YOUR_CONSUMER_KEY",
            consumer_secret="YOUR_CONSUMER_SECRET",
            username="YOUR_USERNAME",
            password="YOUR_PASSWORD",
        )

        async with SalesforcePubSubClient(auth) as client:
            async for event in client.subscribe("/event/Your_Event__e"):
                print(event["id"], event["payload"])


    asyncio.run(main())

The client is an asynchronous context manager: entering it authenticates and
opens the gRPC channel, leaving it closes the channel and stops every active
subscription.

Topic names
-----------

Two kinds of topic are available:

``/event/My_Event__e``
    A `Platform Event <PlatformEvents_>`_.

``/data/AccountChangeEvent``
    A `Change Data Capture <ChangeDataCapture_>`_ event.

Events
------

Each event is a mapping:

``replay_id``
    The event's position, as opaque :class:`bytes`. Pass it to
    :meth:`~simple_salesforce_pubsub.SalesforcePubSubClient.commit_replay` to
    record it, or store it yourself.

``id``
    The event id assigned by Salesforce.

``schema_id``
    The id of the Avro schema the payload was encoded with.

``headers``
    The event headers, as a mapping of :class:`str` to :class:`bytes`.

``payload``
    The Avro-decoded record, as a mapping.

Authenticating
--------------

Three OAuth 2.0 flows are supported.
:obj:`~simple_salesforce_pubsub.PasswordAuthenticator` uses the `username
password flow <password_auth_>`_, which Salesforce discourages for new
integrations:

.. code-block:: python

    PasswordAuthenticator(
        consumer_key="...",
        consumer_secret="...",
        username="...",
        password="...",
        sandbox=True,
    )

:obj:`~simple_salesforce_pubsub.RefreshTokenAuthenticator` uses the `refresh
token flow <refresh_auth_>`_, with a refresh token you obtained earlier:

.. code-block:: python

    RefreshTokenAuthenticator(
        consumer_key="...", consumer_secret="...", refresh_token="..."
    )

:obj:`~simple_salesforce_pubsub.ClientCredentialsAuthenticator` uses the
`client credentials flow <client_credentials_auth_>`_, which sends no user
credentials at all; the user it acts as is configured on the Salesforce side.
Salesforce only issues these tokens from an org's `My Domain <my_domain_>`_
host, so the domain is required and ``login`` and ``test`` are rejected:

.. code-block:: python

    ClientCredentialsAuthenticator(
        consumer_key="...", consumer_secret="...", domain="mycompany.my"
    )

The ``sandbox`` argument of the first two selects ``test.salesforce.com``
instead of ``login.salesforce.com``.

Publishing
----------

.. code-block:: python

    response = await client.publish("/event/Your_Event__e", [{"Field__c": "x"}])
    print(response.results[0].replay_id)

A publish request can half succeed, so a rejected record raises
:obj:`~simple_salesforce_pubsub.PublishError` by default. The whole response
is kept on the exception, which is where the accepted records' replay ids are:

.. code-block:: python

    from simple_salesforce_pubsub import PublishError

    try:
        await client.publish(topic, records)
    except PublishError as error:
        for accepted in error.response.results:
            if not accepted.HasField("error"):
                print("published", accepted.replay_id)
        for rejected in error.errors:
            print("rejected", rejected.msg)

Pass ``raise_on_error=False`` to get the response back instead.

.. include:: global.rst
