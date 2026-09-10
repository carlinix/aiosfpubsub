"""Exception types

Exception hierarchy::

    PubSubException
        AuthenticationError
        ClientError
            ClientInvalidOperation
            PublishError
        SchemaError
        ReplayError
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import cycle avoided at runtime
    from . import pubsub_api_pb2 as pb2


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


class PublishError(ClientError):
    """One or more records of a publish request were rejected

    A publish request can half succeed: the server answers with a result per
    record, and the records that were accepted carry a usable replay id. The
    whole response is therefore kept on the exception rather than discarded.
    """

    def __init__(self, message: str, response: "pb2.PublishResponse") -> None:
        """
        :param message: Error description
        :param response: The server's response, results included
        """
        super().__init__(message, response)
        #: The server's response, with one result per submitted record
        self.response = response

    @property
    def errors(self) -> list["pb2.Error"]:
        """The errors of the rejected records, in submission order"""
        return [
            result.error for result in self.response.results if result.HasField("error")
        ]


class SchemaError(PubSubException):
    """Avro schema fetching or decoding failure"""


class ReplayError(PubSubException):
    """Event replay or replay marker storage related error"""
