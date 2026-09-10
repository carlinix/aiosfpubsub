import asyncio
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from simple_salesforce_pubsub import pubsub_api_pb2 as pb2
from simple_salesforce_pubsub.auth import AuthenticatorBase
from simple_salesforce_pubsub.client import (
    ReplayMarkerStoragePolicy,
    SalesforcePubSubClient,
)
from simple_salesforce_pubsub.exceptions import (
    AuthenticationError,
    ClientError,
    ClientInvalidOperation,
)
from simple_salesforce_pubsub.replay import MappingStorage, ReplayOption

METADATA = (
    ("accesstoken", "token"),
    ("instanceurl", "https://example.my.salesforce.com"),
    ("tenantid", "00D000000000000EAA"),
)


class AuthenticatorStub(AuthenticatorBase):
    def __init__(self):
        super().__init__()
        self.authenticate_calls = 0
        self.access_token = "token"
        self.instance_url = "https://example.my.salesforce.com"
        self.tenant_id = "00D000000000000EAA"

    async def _authenticate(self):  # pragma: no cover - never reached
        raise AssertionError("_authenticate should not be called")

    async def authenticate(self):
        self.authenticate_calls += 1
        self.access_token = f"token-{self.authenticate_calls}"


class RpcErrorStub(grpc.aio.AioRpcError):
    def __init__(self, code, details="boom"):
        super().__init__(code, MagicMock(), MagicMock(), details=details)


def consumer_event(replay_id, event_id="evt"):
    return pb2.ConsumerEvent(
        event=pb2.ProducerEvent(id=event_id, schema_id="schema-1", payload=b""),
        replay_id=replay_id,
    )


def fetch_response(events=(), *, pending=5, latest_replay_id=b""):
    return pb2.FetchResponse(
        events=list(events),
        pending_num_requested=pending,
        latest_replay_id=latest_replay_id,
    )


class ResponseStreamStub:
    """Stands in for the stream returned by a streaming stub method

    A background task drains the client's request stream into
    :obj:`requests`, so that flow control replenishment and commits can be
    asserted on without the stub ever blocking on an empty request queue.
    """

    def __init__(self, responses, error=None):
        self.responses = list(responses)
        self.error = error
        self.requests = []

    def __call__(self, request_iterator, metadata=None):
        self.request_iterator = request_iterator
        self.metadata = metadata
        return self._iterate()

    async def _pump(self):
        async for request in self.request_iterator:
            self.requests.append(request)

    async def _iterate(self):
        pump = asyncio.create_task(self._pump())
        try:
            await self._settle()
            for response in self.responses:
                yield response
                await self._settle()
            if self.error is not None:
                raise self.error
        finally:
            pump.cancel()

    @staticmethod
    async def _settle():
        """Let the pump task consume whatever the client has queued

        The client enqueues requests without suspending, so no wall clock
        wait is needed - only a few scheduling turns.
        """
        for _ in range(3):
            await asyncio.sleep(0)


@pytest.fixture
def client():
    instance = SalesforcePubSubClient(AuthenticatorStub(), replay={})
    instance.stub = MagicMock()
    instance._schema_cache["schema-1"] = {"type": "record", "name": "E", "fields": []}
    return instance


def test_init_rejects_bad_authenticator():
    with pytest.raises(TypeError):
        SalesforcePubSubClient(object())


def test_init_rejects_bad_replay_parameter():
    with pytest.raises(TypeError):
        SalesforcePubSubClient(AuthenticatorStub(), replay=object())


def test_init_accepts_replay_option_and_mapping():
    mapping = {}
    from_mapping = SalesforcePubSubClient(AuthenticatorStub(), replay=mapping)
    from_option = SalesforcePubSubClient(
        AuthenticatorStub(), replay=ReplayOption.ALL_EVENTS
    )

    assert isinstance(from_mapping.replay_storage, MappingStorage)
    assert from_option.replay_storage.default_option is ReplayOption.ALL_EVENTS


@pytest.mark.asyncio
async def test_operations_require_an_open_client():
    closed = SalesforcePubSubClient(AuthenticatorStub())

    with pytest.raises(ClientInvalidOperation):
        await closed.get_topic_info("/event/X__e")
    with pytest.raises(ClientInvalidOperation):
        await anext(closed.subscribe("/event/X__e"))
    with pytest.raises(ClientInvalidOperation):
        closed.managed_subscribe(developer_name="sub")


@pytest.mark.asyncio
async def test_subscribe_yields_decoded_events(client):
    stream = ResponseStreamStub([fetch_response([consumer_event(b"\x01")])])
    client.stub.Subscribe = stream

    events = [event async for event in client.subscribe("/event/X__e")]

    assert events == [
        {
            "replay_id": b"\x01",
            "id": "evt",
            "schema_id": "schema-1",
            "headers": {},
            "payload": {},
        }
    ]
    assert stream.metadata == METADATA


@pytest.mark.asyncio
async def test_subscribe_sends_stored_replay_position(client):
    await client.replay_storage.set_replay_marker("/event/X__e", b"\x09")
    stream = ResponseStreamStub([])
    client.stub.Subscribe = stream

    assert [event async for event in client.subscribe("/event/X__e")] == []

    assert stream.requests[0].replay_preset == pb2.CUSTOM
    assert stream.requests[0].replay_id == b"\x09"
    assert stream.requests[0].num_requested == 10


@pytest.mark.asyncio
async def test_subscribe_replenishes_when_budget_is_exhausted(client):
    stream = ResponseStreamStub(
        [
            fetch_response([consumer_event(b"\x01")], pending=1),
            fetch_response([consumer_event(b"\x02")], pending=0),
        ]
    )
    client.stub.Subscribe = stream

    async for _ in client.subscribe("/event/X__e", num_requested=3):
        pass

    # the initial request, then one replenishment for the exhausted budget
    assert len(stream.requests) == 2
    assert stream.requests[1].num_requested == 3
    assert stream.requests[1].topic_name == "/event/X__e"


@pytest.mark.asyncio
async def test_automatic_policy_stores_marker_of_consumed_events(client):
    client.stub.Subscribe = ResponseStreamStub(
        [fetch_response([consumer_event(b"\x01"), consumer_event(b"\x02")])]
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert await client.replay_storage.get_replay_marker("/event/X__e") == b"\x02"


@pytest.mark.asyncio
async def test_automatic_policy_advances_on_keepalive(client):
    client.stub.Subscribe = ResponseStreamStub(
        [fetch_response(latest_replay_id=b"\x07")]
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert await client.replay_storage.get_replay_marker("/event/X__e") == b"\x07"


@pytest.mark.asyncio
async def test_manual_policy_stores_nothing_until_committed(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    client.stub.Subscribe = ResponseStreamStub(
        [fetch_response([consumer_event(b"\x01")], latest_replay_id=b"\x07")]
    )

    async for event in client.subscribe("/event/X__e"):
        assert await client.replay_storage.get_replay_marker("/event/X__e") is None
        await client.commit_replay("/event/X__e", event["replay_id"])

    assert await client.replay_storage.get_replay_marker("/event/X__e") == b"\x01"


@pytest.mark.asyncio
async def test_manual_policy_ignores_keepalive(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    client.stub.Subscribe = ResponseStreamStub(
        [fetch_response(latest_replay_id=b"\x07")]
    )

    async for _ in client.subscribe("/event/X__e"):  # pragma: no cover - no events
        pass

    assert await client.replay_storage.get_replay_marker("/event/X__e") is None


@pytest.mark.asyncio
async def test_subscribe_reauthenticates_and_resumes(client):
    expired = ResponseStreamStub(
        [fetch_response([consumer_event(b"\x01")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = ResponseStreamStub([fetch_response([consumer_event(b"\x02")])])
    streams = iter([expired, resumed])
    client.stub.Subscribe = lambda *args, **kwargs: next(streams)(*args, **kwargs)

    events = [event async for event in client.subscribe("/event/X__e")]

    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    assert client.authenticator.authenticate_calls == 1
    # the resumed stream picks up after the last stored marker
    assert resumed.requests[0].replay_preset == pb2.CUSTOM
    assert resumed.requests[0].replay_id == b"\x01"
    assert resumed.requests[0].auth_refresh == "token-1"


@pytest.mark.asyncio
async def test_subscribe_wraps_other_rpc_errors(client):
    client.stub.Subscribe = ResponseStreamStub(
        [], error=RpcErrorStub(grpc.StatusCode.PERMISSION_DENIED, "denied")
    )

    with pytest.raises(ClientError, match="denied"):
        async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
            pass


@pytest.mark.asyncio
async def test_managed_subscribe_requires_an_identifier(client):
    with pytest.raises(ValueError):
        client.managed_subscribe()


@pytest.mark.asyncio
async def test_managed_subscribe_commits_automatically(client):
    stream = ResponseStreamStub(
        [
            pb2.ManagedFetchResponse(
                events=[consumer_event(b"\x01")], pending_num_requested=5
            )
        ]
    )
    client.stub.ManagedSubscribe = stream

    subscription = client.managed_subscribe(developer_name="sub")
    events = [event async for event in subscription]

    assert [event["replay_id"] for event in events] == [b"\x01"]
    assert stream.requests[0].developer_name == "sub"
    commit = stream.requests[1].commit_replay_id_request
    assert commit.replay_id == b"\x01"
    assert commit.commit_request_id


@pytest.mark.asyncio
async def test_managed_subscribe_manual_policy_does_not_commit(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    stream = ResponseStreamStub(
        [
            pb2.ManagedFetchResponse(
                events=[consumer_event(b"\x01")], pending_num_requested=0
            )
        ]
    )
    client.stub.ManagedSubscribe = stream

    async for _ in client.managed_subscribe(subscription_id="sub-id"):
        pass

    # the only follow-up request is the flow control replenishment
    assert not stream.requests[1].HasField("commit_replay_id_request")
    assert stream.requests[1].num_requested == 10


@pytest.mark.asyncio
async def test_managed_subscribe_records_commit_responses(client):
    stream = ResponseStreamStub(
        [
            pb2.ManagedFetchResponse(
                commit_response=pb2.CommitReplayResponse(
                    commit_request_id="abc", replay_id=b"\x01"
                ),
                pending_num_requested=5,
            )
        ]
    )
    client.stub.ManagedSubscribe = stream

    subscription = client.managed_subscribe(developer_name="sub")
    async for _ in subscription:  # pragma: no cover - no events
        pass

    assert subscription.commit_responses["abc"].replay_id == b"\x01"


@pytest.mark.asyncio
async def test_publish_encodes_records_with_the_topic_schema(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    client.stub.Publish = AsyncMock(
        return_value=pb2.PublishResponse(
            results=[pb2.PublishResult(replay_id=b"\x01")], schema_id="schema-1"
        )
    )

    response = await client.publish("/event/X__e", [{}])

    request = client.stub.Publish.await_args.args[0]
    assert request.topic_name == "/event/X__e"
    assert [event.schema_id for event in request.events] == ["schema-1"]
    assert response.results[0].replay_id == b"\x01"


@pytest.mark.asyncio
async def test_publish_wraps_rpc_errors(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    client.stub.Publish = AsyncMock(
        side_effect=RpcErrorStub(grpc.StatusCode.PERMISSION_DENIED, "denied")
    )

    with pytest.raises(ClientError, match="denied"):
        await client.publish("/event/X__e", [{}])


@pytest.mark.asyncio
async def test_subscribe_gives_up_after_repeated_auth_failures(client):
    client.auth_retries = 2

    def always_unauthenticated(*args, **kwargs):
        return ResponseStreamStub(
            [], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)
        )(*args, **kwargs)

    client.stub.Subscribe = always_unauthenticated

    with pytest.raises(AuthenticationError, match="2 consecutive"):
        async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
            pass

    assert client.authenticator.authenticate_calls == 2


@pytest.mark.asyncio
async def test_subscribe_resets_the_retry_count_after_delivering_events(client):
    streams = iter(
        [
            ResponseStreamStub(
                [], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)
            ),
            ResponseStreamStub(
                [fetch_response([consumer_event(b"\x01")])],
                error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
            ),
            ResponseStreamStub(
                [], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)
            ),
            ResponseStreamStub([fetch_response([consumer_event(b"\x02")])]),
        ]
    )
    client.auth_retries = 1
    client.stub.Subscribe = lambda *args, **kwargs: next(streams)(*args, **kwargs)

    events = [event async for event in client.subscribe("/event/X__e")]

    # without the reset, the third failure would exhaust the single retry
    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    assert client.authenticator.authenticate_calls == 3
