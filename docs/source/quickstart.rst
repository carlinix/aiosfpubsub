Quickstart
==========

Subscribing
-----------

Create an authenticator, hand it to a
:obj:`~aiosfpubsub.SalesforcePubSubClient`, and iterate a
subscription:

.. code-block:: python

    import asyncio

    from aiosfpubsub import (
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
    :meth:`~aiosfpubsub.SalesforcePubSubClient.commit_replay` to
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

Four OAuth 2.0 flows are supported, plus the SOAP API's ``login()`` call.
:obj:`~aiosfpubsub.PasswordAuthenticator` uses the `username
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

:obj:`~aiosfpubsub.RefreshTokenAuthenticator` uses the `refresh
token flow <refresh_auth_>`_, with a refresh token you obtained earlier:

.. code-block:: python

    RefreshTokenAuthenticator(
        consumer_key="...", consumer_secret="...", refresh_token="..."
    )

:obj:`~aiosfpubsub.ClientCredentialsAuthenticator` uses the
`client credentials flow <client_credentials_auth_>`_, which sends no user
credentials at all; the user it acts as is configured on the Salesforce side.
Salesforce only issues these tokens from an org's `My Domain <my_domain_>`_
host, so the domain is required and ``login`` and ``test`` are rejected:

.. code-block:: python

    ClientCredentialsAuthenticator(
        consumer_key="...", consumer_secret="...", domain="mycompany.my"
    )

:obj:`~aiosfpubsub.JWTBearerAuthenticator` uses the `JWT Bearer flow
<jwt_auth_>`_, the flow Salesforce recommends for server-to-server
integrations. It sends neither a password nor a consumer secret: the client
signs a short lived assertion with an RSA private key, whose certificate is
uploaded to the app definition, and the user named by ``username`` has to be
pre-authorized for that app. Signing requires PyJWT_ with its cryptography
backend, which comes with the ``jwt`` extra
(``pip install aiosfpubsub[jwt]``):

.. code-block:: python

    JWTBearerAuthenticator(
        consumer_key="...", username="...", private_key_path="/path/to/server.key"
    )

The key can also be passed directly as a PEM formatted string, with the
``private_key`` argument, which is the more convenient option when it is read
from a secret store rather than from a file.

The ``sandbox`` argument of every flow but the client credentials one selects
``test.salesforce.com`` instead of ``login.salesforce.com``; for the JWT
Bearer flow it also selects the matching ``aud`` claim.

:obj:`~aiosfpubsub.SOAPAuthenticator` is the odd one out: it uses the SOAP
API's `login() <soap_login_>`_ call rather than OAuth, and so needs no
connected app at all. It exchanges a username and a password for a session
ID, which the Pub/Sub API accepts in place of an access token. This is what
Salesforce's own Pub/Sub API reference client does:

.. code-block:: python

    SOAPAuthenticator(
        username="...", password="...", security_token="..."
    )

The security token is appended to the password, and is required unless the
caller's IP falls inside the trusted IP range of the user's profile. Pass
``domain`` to log in against a My Domain host instead of
``login.salesforce.com``.

.. warning::

    ``login()`` is already unavailable in SOAP API version 65.0 and later,
    and Salesforce `retires it <soap_login_retirement_>`_ from versions 31.0
    through 64.0 in the Summer '27 release. Use
    :obj:`~aiosfpubsub.JWTBearerAuthenticator` or
    :obj:`~aiosfpubsub.ClientCredentialsAuthenticator` for new integrations.

Publishing
----------

.. code-block:: python

    response = await client.publish("/event/Your_Event__e", [{"Field__c": "x"}])
    print(response.results[0].replay_id)

A publish request can half succeed, so a rejected record raises
:obj:`~aiosfpubsub.PublishError` by default. The whole response
is kept on the exception, which is where the accepted records' replay ids are:

.. code-block:: python

    from aiosfpubsub import PublishError

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
