Installation
============

.. code-block:: console

    $ pip install simple-salesforce-pubsub

Python 3.11 or newer is required.

The package depends on grpcio_, protobuf_, aiohttp_ and fastavro_, all of
which pip installs for you. The Pub/Sub API stubs are generated from
Salesforce's ``pubsub_api.proto`` and ship with the package, so
``grpcio-tools`` is only needed to regenerate them.

Salesforce setup
----------------

The client authenticates with OAuth 2.0 against a `connected app
<connected_app_>`_ in your org. The app supplies the consumer key and consumer
secret the authenticators take, and its scopes have to allow the Pub/Sub API.

.. _grpcio: https://grpc.io/
.. _protobuf: https://protobuf.dev/

.. include:: global.rst
