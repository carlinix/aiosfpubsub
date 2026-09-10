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

Change Data Capture
-------------------

.. automodule:: simple_salesforce_pubsub.cdc

.. py:currentmodule:: simple_salesforce_pubsub

.. autofunction:: expand_change_event_header

.. autofunction:: expand_bitmap_fields

.. autofunction:: expand_bitmap

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
