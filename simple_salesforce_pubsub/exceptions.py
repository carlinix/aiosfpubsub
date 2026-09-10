"""Exception types

Exception hierarchy::

    PubSubException
        AuthenticationError
        ClientError
            ClientInvalidOperation
        SchemaError
        ReplayError
"""


class PubSubException(Exception):
    """Base exception type.

    All exceptions of the package inherit from this class.
    """


class AuthenticationError(PubSubException):
    """Authentication failure"""


class ClientError(PubSubException):
    """Client side error"""


class ClientInvalidOperation(ClientError):
    """The requested operation can't be executed on the current state of the
    client"""


class SchemaError(PubSubException):
    """Avro schema fetching or decoding failure"""


class ReplayError(PubSubException):
    """Event replay or replay marker storage related error"""
