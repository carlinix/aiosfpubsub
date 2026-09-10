import pytest

from aiosfpubsub.exceptions import (
    AuthenticationError,
    ClientError,
    ClientInvalidOperation,
    PubSubException,
    ReplayError,
    SchemaError,
)


@pytest.mark.parametrize(
    "error_class",
    [AuthenticationError, ClientError, SchemaError, ReplayError],
)
def test_every_error_derives_from_the_base(error_class):
    assert issubclass(error_class, PubSubException)


def test_client_invalid_operation_is_a_client_error():
    assert issubclass(ClientInvalidOperation, ClientError)
