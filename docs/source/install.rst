Installation
============

.. code-block:: console

    $ pip install aiosfpubsub

Python 3.11 or newer is required.

The package depends on grpcio_, protobuf_, aiohttp_ and fastavro_, all of
which pip installs for you. The Pub/Sub API stubs are generated from
Salesforce's ``pubsub_api.proto`` and ship with the package, so
``grpcio-tools`` is only needed to regenerate them.

:obj:`~aiosfpubsub.JWTBearerAuthenticator` signs its assertion with PyJWT_,
which is not installed by default:

.. code-block:: console

    $ pip install aiosfpubsub[jwt]

Salesforce setup
----------------

Most of the authenticators use OAuth 2.0 against a `connected app
<connected_app_>`_ in your org. The app supplies the consumer key and consumer
secret they take, and its scopes have to allow the Pub/Sub API.

:obj:`~aiosfpubsub.SOAPAuthenticator` is the exception: it uses the SOAP API's
``login()`` call and needs no connected app. Salesforce retires that call in
Summer '27, so it is a fallback for orgs where a connected app is out of
reach, not a starting point.

.. _grpcio: https://grpc.io/
.. _protobuf: https://protobuf.dev/

.. include:: global.rst
