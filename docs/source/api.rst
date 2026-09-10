API Reference
=============

.. py:currentmodule:: simple_salesforce_pubsub

Client
------

.. autoclass:: SalesforcePubSubClient
    :members:

.. autoclass:: ManagedSubscription
    :members:

.. autoclass:: ReplayMarkerStoragePolicy
    :members:
    :undoc-members:

Authenticators
--------------

.. autoclass:: AuthenticatorBase
    :members:

.. autoclass:: PasswordAuthenticator
    :members:

.. autoclass:: RefreshTokenAuthenticator
    :members:

.. autoclass:: ClientCredentialsAuthenticator
    :members:

Replay
------

.. autoclass:: ReplayOption
    :members:
    :undoc-members:

.. autoclass:: ReplayMarkerStorage
    :members:

.. autoclass:: MappingStorage

.. autoclass:: ConstantReplayId

Exceptions
----------

.. automodule:: simple_salesforce_pubsub.exceptions

.. py:currentmodule:: simple_salesforce_pubsub.exceptions

.. autoexception:: PubSubException
    :members:

.. autoexception:: AuthenticationError
    :members:

.. autoexception:: ClientError
    :members:

.. autoexception:: ClientInvalidOperation
    :members:

.. autoexception:: PublishError
    :members:

.. autoexception:: SchemaError
    :members:

.. autoexception:: ReplayError
    :members:
