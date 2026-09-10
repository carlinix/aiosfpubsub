import asyncio
import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import fastavro
import grpc
import pytest

from aiosfpubsub import pubsub_api_pb2 as pb2
from aiosfpubsub.auth import AuthenticatorBase
from aiosfpubsub.client import (
    ReplayMarkerStoragePolicy,
    SalesforcePubSubClient,
    _StreamSlot,
)
from aiosfpubsub.exceptions import (
    AuthenticationError,
    ClientError,
    ClientInvalidOperation,
    PublishError,
    SchemaError,
)
from aiosfpubsub.replay import MappingStorage, ReplayOption

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
    def __init__(self, code, details="boom", trailers=()):
        super().__init__(
            code,
            MagicMock(),
            grpc.aio.Metadata(*trailers),
            details=details,
        )


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


class CallStub:
    """Stands in for the call object returned by a streaming stub method

    A background task drains the client's request stream into
    :obj:`requests`, so that flow control replenishment and commits can be
    asserted on without the stub ever blocking on an empty request queue.
    """

    def __init__(self, responses, error=None, end_with="cancel"):
        """
        :param end_with: what happens once the responses run out -
            "cancel" mimics the consumer stopping the subscription, which is
            the only clean end; "close" mimics the server closing the stream,
            which the client answers by re-establishing it.
        """
        self.responses = list(responses)
        self.error = error
        self.end_with = end_with
        self.requests = []
        self.cancelled = False

    def __call__(self, request_iterator, metadata=None):
        self.request_iterator = request_iterator
        self.metadata = metadata
        return self

    def cancel(self):
        self.cancelled = True

    async def _pump(self):
        async for request in self.request_iterator:
            self.requests.append(request)

    async def __aiter__(self):
        pump = asyncio.create_task(self._pump())
        try:
            await self._settle()
            for response in self.responses:
                if self.cancelled:
                    break
                yield response
                await self._settle()
            if self.error is not None:
                raise self.error
            if self.cancelled or self.end_with == "cancel":
                raise RpcErrorStub(grpc.StatusCode.CANCELLED, "cancelled")
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


def call_sequence(*calls):
    """Return a stub method handing out *calls* one per invocation"""
    remaining = iter(calls)
    return lambda *args, **kwargs: next(remaining)(*args, **kwargs)


@pytest.fixture
def client():
    instance = SalesforcePubSubClient(AuthenticatorStub(), replay={})
    instance.stub = MagicMock()
    instance._schema_cache["schema-1"] = {"type": "record", "name": "E", "fields": []}
    # keep retries instant and deterministic; the schedule is tested separately
    instance.backoff_delay = lambda attempt: 0.0
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
    stream = CallStub([fetch_response([consumer_event(b"\x01")])])
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
    stream = CallStub([])
    client.stub.Subscribe = stream

    assert [event async for event in client.subscribe("/event/X__e")] == []

    assert stream.requests[0].replay_preset == pb2.CUSTOM
    assert stream.requests[0].replay_id == b"\x09"
    assert stream.requests[0].num_requested == 10


@pytest.mark.asyncio
async def test_subscribe_replenishes_when_budget_is_exhausted(client):
    stream = CallStub(
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
    client.stub.Subscribe = CallStub(
        [fetch_response([consumer_event(b"\x01"), consumer_event(b"\x02")])]
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert await client.replay_storage.get_replay_marker("/event/X__e") == b"\x02"


@pytest.mark.asyncio
async def test_automatic_policy_advances_on_keepalive(client):
    client.stub.Subscribe = CallStub([fetch_response(latest_replay_id=b"\x07")])

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert await client.replay_storage.get_replay_marker("/event/X__e") == b"\x07"


@pytest.mark.asyncio
async def test_manual_policy_stores_nothing_until_committed(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    client.stub.Subscribe = CallStub(
        [fetch_response([consumer_event(b"\x01")], latest_replay_id=b"\x07")]
    )

    async for event in client.subscribe("/event/X__e"):
        assert await client.replay_storage.get_replay_marker("/event/X__e") is None
        await client.commit_replay("/event/X__e", event["replay_id"])

    assert await client.replay_storage.get_replay_marker("/event/X__e") == b"\x01"


@pytest.mark.asyncio
async def test_manual_policy_ignores_keepalive(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    client.stub.Subscribe = CallStub([fetch_response(latest_replay_id=b"\x07")])

    async for _ in client.subscribe("/event/X__e"):  # pragma: no cover - no events
        pass

    assert await client.replay_storage.get_replay_marker("/event/X__e") is None


@pytest.mark.asyncio
async def test_subscribe_reauthenticates_and_resumes(client):
    expired = CallStub(
        [fetch_response([consumer_event(b"\x01")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([fetch_response([consumer_event(b"\x02")])])
    client.stub.Subscribe = call_sequence(expired, resumed)

    events = [event async for event in client.subscribe("/event/X__e")]

    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    assert client.authenticator.authenticate_calls == 1
    # the resumed stream picks up after the last stored marker
    assert resumed.requests[0].replay_preset == pb2.CUSTOM
    assert resumed.requests[0].replay_id == b"\x01"


@pytest.mark.asyncio
async def test_subscribe_wraps_other_rpc_errors(client):
    client.stub.Subscribe = CallStub(
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
    stream = CallStub(
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
    stream = CallStub(
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
    stream = CallStub(
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

    client.stub.Subscribe = lambda *args, **kwargs: CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)
    )(*args, **kwargs)

    with pytest.raises(AuthenticationError, match="2 consecutive"):
        async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
            pass

    assert client.authenticator.authenticate_calls == 2


@pytest.mark.asyncio
async def test_subscribe_resets_the_retry_count_after_delivering_events(client):
    client.auth_retries = 1
    client.stub.Subscribe = call_sequence(
        CallStub([], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)),
        CallStub(
            [fetch_response([consumer_event(b"\x01")])],
            error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
        ),
        CallStub([], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)),
        CallStub([fetch_response([consumer_event(b"\x02")])]),
    )

    events = [event async for event in client.subscribe("/event/X__e")]

    # without the reset, the third failure would exhaust the single retry
    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    assert client.authenticator.authenticate_calls == 3


@pytest.mark.asyncio
async def test_manual_policy_redelivers_uncommitted_events(client):
    """Uncommitted events must come back after a re-authentication

    The consumer received an event but never committed it, so the stored
    marker still points before it and the resumed stream redelivers it.
    """
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    await client.replay_storage.set_replay_marker("/event/X__e", b"\x05")
    expired = CallStub(
        [fetch_response([consumer_event(b"\x06")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([fetch_response([consumer_event(b"\x06")])])
    client.stub.Subscribe = call_sequence(expired, resumed)

    events = [event async for event in client.subscribe("/event/X__e")]

    assert [event["replay_id"] for event in events] == [b"\x06", b"\x06"]
    assert resumed.requests[0].replay_preset == pb2.CUSTOM
    assert resumed.requests[0].replay_id == b"\x05"


@pytest.mark.asyncio
async def test_manual_policy_without_a_marker_cannot_anchor_before_an_event(client):
    """A known limitation, asserted so a change to it is deliberate

    Under NEW_EVENTS the subscription only learns a concrete replay id from
    the events themselves. If the very first response already carries events
    and none of them is committed, there is no position that precedes them,
    so the resumed stream starts from NEW_EVENTS again.
    """
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    expired = CallStub(
        [fetch_response([consumer_event(b"\x01")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([])
    client.stub.Subscribe = call_sequence(expired, resumed)

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert resumed.requests[0].replay_preset == pb2.LATEST


@pytest.mark.asyncio
async def test_resume_uses_the_keepalive_anchor_for_new_events(client):
    """A NEW_EVENTS subscription anchors itself to a concrete replay id

    Re-establishing it with NEW_EVENTS would mean "from now" all over again,
    skipping whatever was published while the token was being refreshed.
    """
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    expired = CallStub(
        [fetch_response(latest_replay_id=b"\x0a")],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([])
    client.stub.Subscribe = call_sequence(expired, resumed)

    async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
        pass

    assert expired.requests[0].replay_preset == pb2.LATEST
    assert resumed.requests[0].replay_preset == pb2.CUSTOM
    assert resumed.requests[0].replay_id == b"\x0a"


@pytest.mark.asyncio
async def test_keepalive_does_not_anchor_after_an_event_was_delivered(client):
    """Anchoring past a delivered but uncommitted event would lose it"""
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    expired = CallStub(
        [
            fetch_response([consumer_event(b"\x01")]),
            fetch_response(latest_replay_id=b"\x0a"),
        ],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([])
    client.stub.Subscribe = call_sequence(expired, resumed)

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert resumed.requests[0].replay_preset == pb2.LATEST


@pytest.mark.asyncio
async def test_resume_without_a_storing_storage(client):
    """ConstantReplayId stores nothing, so the resume position is in memory"""
    client = SalesforcePubSubClient(AuthenticatorStub(), replay=ReplayOption.ALL_EVENTS)
    client.stub = MagicMock()
    client._schema_cache["schema-1"] = {"type": "record", "name": "E", "fields": []}
    expired = CallStub(
        [fetch_response([consumer_event(b"\x03")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([])
    client.stub.Subscribe = call_sequence(expired, resumed)

    async for _ in client.subscribe("/event/X__e"):
        pass

    # without the in-memory position this would re-read the whole window
    assert expired.requests[0].replay_preset == pb2.EARLIEST
    assert resumed.requests[0].replay_preset == pb2.CUSTOM
    assert resumed.requests[0].replay_id == b"\x03"


def test_is_replay_id_error_reads_the_salesforce_trailer():
    """Salesforce reports its own code in the error-code trailing metadata"""
    assert SalesforcePubSubClient.is_replay_id_error(
        RpcErrorStub(
            grpc.StatusCode.INVALID_ARGUMENT,
            "an opaque description",
            trailers=(
                (
                    "error-code",
                    "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted",
                ),
            ),
        )
    )


def test_is_replay_id_error_falls_back_to_the_description():
    assert SalesforcePubSubClient.is_replay_id_error(
        RpcErrorStub(grpc.StatusCode.INVALID_ARGUMENT, "Replay ID is invalid")
    )


def test_is_replay_id_error_ignores_other_failures():
    assert not SalesforcePubSubClient.is_replay_id_error(
        RpcErrorStub(grpc.StatusCode.INVALID_ARGUMENT, "Topic does not exist")
    )
    assert not SalesforcePubSubClient.is_replay_id_error(
        RpcErrorStub(
            grpc.StatusCode.INVALID_ARGUMENT,
            "Topic does not exist",
            trailers=(("error-code", "sfdc.platform.eventbus.grpc.topic.notfound"),),
        )
    )
    assert not SalesforcePubSubClient.is_replay_id_error(
        RpcErrorStub(grpc.StatusCode.PERMISSION_DENIED, "replay")
    )


@pytest.mark.asyncio
async def test_replay_fallback_retries_and_clears_the_marker(client):
    client.replay_fallback = ReplayOption.ALL_EVENTS
    await client.replay_storage.set_replay_marker("/event/X__e", b"\x99")
    rejected = CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.INVALID_ARGUMENT, "Replay ID is stale")
    )
    retried = CallStub([fetch_response([consumer_event(b"\x01")])])
    client.stub.Subscribe = call_sequence(rejected, retried)

    events = [event async for event in client.subscribe("/event/X__e")]

    assert [event["replay_id"] for event in events] == [b"\x01"]
    assert rejected.requests[0].replay_id == b"\x99"
    assert retried.requests[0].replay_preset == pb2.EARLIEST
    assert retried.requests[0].replay_id == b""


@pytest.mark.asyncio
async def test_replay_fallback_can_be_given_per_subscription(client):
    rejected = CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.INVALID_ARGUMENT, "Replay ID is stale")
    )
    retried = CallStub([])
    client.stub.Subscribe = call_sequence(rejected, retried)

    async for _ in client.subscribe(  # pragma: no cover
        "/event/X__e", replay_fallback=ReplayOption.ALL_EVENTS
    ):
        pass

    assert retried.requests[0].replay_preset == pb2.EARLIEST


@pytest.mark.asyncio
async def test_replay_id_error_without_a_fallback_is_raised(client):
    client.stub.Subscribe = lambda *args, **kwargs: CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.INVALID_ARGUMENT, "Replay ID is stale")
    )(*args, **kwargs)

    with pytest.raises(ClientError, match="Replay ID is stale"):
        async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
            pass


@pytest.mark.asyncio
async def test_replay_fallback_is_not_retried_twice(client):
    client.replay_fallback = ReplayOption.ALL_EVENTS
    error = RpcErrorStub(grpc.StatusCode.INVALID_ARGUMENT, "Replay ID is stale")
    client.stub.Subscribe = call_sequence(
        CallStub([], error=error), CallStub([], error=error)
    )

    with pytest.raises(ClientError, match="Replay ID is stale"):
        async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
            pass


@pytest.mark.asyncio
async def test_unsubscribe_ends_the_iteration(client):
    call = CallStub(
        [
            fetch_response([consumer_event(b"\x01")]),
            fetch_response([consumer_event(b"\x02")]),
        ]
    )
    client.stub.Subscribe = call

    events = []
    async for event in client.subscribe("/event/X__e"):
        events.append(event)
        client.unsubscribe("/event/X__e")

    assert [event["replay_id"] for event in events] == [b"\x01"]
    assert call.cancelled


@pytest.mark.asyncio
async def test_unsubscribe_reports_whether_there_was_a_subscription(client):
    assert client.unsubscribe("/event/X__e") is False


@pytest.mark.asyncio
async def test_subscriptions_are_tracked_and_released(client):
    client.stub.Subscribe = CallStub([fetch_response([consumer_event(b"\x01")])])

    events = client.subscribe("/event/X__e")
    await anext(events)
    assert client.subscriptions == frozenset({"/event/X__e"})

    await events.aclose()
    assert client.subscriptions == frozenset()


@pytest.mark.asyncio
async def test_subscribing_twice_to_the_same_topic_is_rejected(client):
    client.stub.Subscribe = CallStub([fetch_response([consumer_event(b"\x01")])])

    events = client.subscribe("/event/X__e")
    await anext(events)

    with pytest.raises(ClientInvalidOperation, match="Already subscribed"):
        await anext(client.subscribe("/event/X__e"))

    await events.aclose()


@pytest.mark.asyncio
async def test_close_cancels_active_subscriptions(client):
    call = CallStub(
        [
            fetch_response([consumer_event(b"\x01")]),
            fetch_response([consumer_event(b"\x02")]),
        ]
    )
    client.stub.Subscribe = call
    client.channel = MagicMock(close=AsyncMock())

    events = client.subscribe("/event/X__e")
    await anext(events)
    await client.close()

    assert call.cancelled
    assert client.subscriptions == frozenset()
    assert [event async for event in events] == []


@pytest.mark.asyncio
async def test_publish_stream_encodes_every_batch(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    call = CallStub(
        [
            pb2.PublishResponse(results=[pb2.PublishResult(replay_id=b"\x01")]),
            pb2.PublishResponse(results=[pb2.PublishResult(replay_id=b"\x02")]),
        ],
        end_with="close",
    )
    client.stub.PublishStream = call

    async def batches():
        yield [{}]
        yield [{}, {}]

    responses = [
        response async for response in client.publish_stream("/event/X__e", batches())
    ]

    assert [response.results[0].replay_id for response in responses] == [
        b"\x01",
        b"\x02",
    ]
    assert [len(request.events) for request in call.requests] == [1, 2]
    assert call.requests[0].topic_name == "/event/X__e"
    assert call.requests[0].events[0].schema_id == "schema-1"


@pytest.mark.asyncio
async def test_publish_stream_wraps_rpc_errors(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    client.stub.PublishStream = CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.PERMISSION_DENIED, "denied")
    )

    async def batches():
        yield [{}]

    with pytest.raises(ClientError, match="denied"):
        async for _ in client.publish_stream("/event/X__e", batches()):
            pass  # pragma: no cover


@pytest.mark.asyncio
async def test_open_authenticates_and_builds_the_stub():
    client = SalesforcePubSubClient(AuthenticatorStub(), endpoint="pubsub.test:443")
    channel = MagicMock()
    with (
        patch("aiosfpubsub.client.grpc.ssl_channel_credentials"),
        patch(
            "aiosfpubsub.client.grpc.aio.secure_channel",
            return_value=channel,
        ) as secure_channel,
    ):
        await client.open()

    assert client.authenticator.authenticate_calls == 1
    assert client.channel is channel
    assert client.stub is not None
    assert secure_channel.call_args.args[0] == "pubsub.test:443"


@pytest.mark.asyncio
async def test_connect_is_an_alias_of_open():
    assert SalesforcePubSubClient.connect is SalesforcePubSubClient.open


@pytest.mark.asyncio
async def test_context_manager_opens_and_closes():
    client = SalesforcePubSubClient(AuthenticatorStub())
    channel = MagicMock(close=AsyncMock())
    with (
        patch("aiosfpubsub.client.grpc.ssl_channel_credentials"),
        patch(
            "aiosfpubsub.client.grpc.aio.secure_channel",
            return_value=channel,
        ),
    ):
        async with client as entered:
            assert entered is client
            assert client.stub is not None

    channel.close.assert_awaited_once()
    assert client.channel is None
    assert client.stub is None


@pytest.mark.asyncio
async def test_close_without_a_channel_is_harmless(client):
    client.channel = None

    await client.close()

    assert client.stub is not None


@pytest.mark.asyncio
async def test_get_topic_info_wraps_rpc_errors(client):
    client.stub.GetTopic = AsyncMock(
        side_effect=RpcErrorStub(grpc.StatusCode.NOT_FOUND, "no topic")
    )

    with pytest.raises(ClientError, match="no topic"):
        await client.get_topic_info("/event/X__e")


@pytest.mark.asyncio
async def test_get_schema_parses_and_caches(client):
    schema_json = json.dumps(
        {"type": "record", "name": "E", "fields": [{"name": "a", "type": "string"}]}
    )
    client.stub.GetSchema = AsyncMock(
        return_value=pb2.SchemaInfo(schema_id="schema-2", schema_json=schema_json)
    )

    first = await client.get_schema("schema-2")
    second = await client.get_schema("schema-2")

    assert first is second
    assert client.stub.GetSchema.await_count == 1
    assert first["name"] == "E"


@pytest.mark.asyncio
async def test_get_schema_wraps_fetch_errors(client):
    client.stub.GetSchema = AsyncMock(
        side_effect=RpcErrorStub(grpc.StatusCode.NOT_FOUND, "no schema")
    )

    with pytest.raises(SchemaError, match="Failed to fetch schema"):
        await client.get_schema("schema-2")


@pytest.mark.asyncio
async def test_get_schema_wraps_parse_errors(client):
    client.stub.GetSchema = AsyncMock(
        return_value=pb2.SchemaInfo(schema_id="schema-2", schema_json="not json")
    )

    with pytest.raises(SchemaError, match="Failed to parse schema"):
        await client.get_schema("schema-2")


@pytest.mark.asyncio
async def test_events_are_decoded_with_their_schema_and_headers(client):
    schema = {
        "type": "record",
        "name": "E",
        "fields": [{"name": "Field__c", "type": "string"}],
    }
    client._schema_cache["schema-2"] = fastavro.parse_schema(schema)
    payload = io.BytesIO()
    fastavro.schemaless_writer(
        payload, client._schema_cache["schema-2"], {"Field__c": "value"}
    )
    event = pb2.ConsumerEvent(
        event=pb2.ProducerEvent(
            id="evt",
            schema_id="schema-2",
            payload=payload.getvalue(),
            headers=[pb2.EventHeader(key="trace", value=b"abc")],
        ),
        replay_id=b"\x01",
    )
    client.stub.Subscribe = CallStub([fetch_response([event])])

    decoded = await anext(client.subscribe("/event/X__e"))

    assert decoded["payload"] == {"Field__c": "value"}
    assert decoded["headers"] == {"trace": b"abc"}
    assert decoded["id"] == "evt"


@pytest.mark.asyncio
async def test_managed_subscribe_wraps_rpc_errors(client):
    client.stub.ManagedSubscribe = CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.PERMISSION_DENIED, "denied")
    )

    with pytest.raises(ClientError, match="Managed subscribe failed"):
        async for _ in client.managed_subscribe(developer_name="sub"):
            pass  # pragma: no cover


def test_managed_subscription_repr(client):
    subscription = client.managed_subscribe(developer_name="sub")

    assert repr(subscription) == (
        "ManagedSubscription(subscription_id='', developer_name='sub')"
    )


@pytest.mark.asyncio
async def test_managed_subscription_is_registered_and_released(client):
    client.stub.ManagedSubscribe = CallStub(
        [pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")])]
    )

    subscription = client.managed_subscribe(developer_name="sub")
    assert client.subscriptions == frozenset()

    events = subscription.__aiter__()
    await anext(events)
    assert client.subscriptions == frozenset({"sub"})

    await events.aclose()
    assert client.subscriptions == frozenset()


@pytest.mark.asyncio
async def test_managed_subscription_cancel_ends_the_iteration(client):
    call = CallStub(
        [
            pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")]),
            pb2.ManagedFetchResponse(events=[consumer_event(b"\x02")]),
        ]
    )
    client.stub.ManagedSubscribe = call

    subscription = client.managed_subscribe(subscription_id="sub-id")
    events = []
    async for event in subscription:
        events.append(event)
        assert subscription.cancel() is True

    assert [event["replay_id"] for event in events] == [b"\x01"]
    assert call.cancelled
    assert subscription.cancel() is False


@pytest.mark.asyncio
async def test_close_cancels_managed_subscriptions(client):
    call = CallStub(
        [
            pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")]),
            pb2.ManagedFetchResponse(events=[consumer_event(b"\x02")]),
        ]
    )
    client.stub.ManagedSubscribe = call
    client.channel = MagicMock(close=AsyncMock())

    events = client.managed_subscribe(developer_name="sub").__aiter__()
    await anext(events)
    await client.close()

    assert call.cancelled
    assert client.subscriptions == frozenset()
    assert [event async for event in events] == []


@pytest.mark.asyncio
async def test_managed_subscribing_twice_under_the_same_name_is_rejected(client):
    client.stub.ManagedSubscribe = CallStub(
        [pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")])]
    )

    events = client.managed_subscribe(developer_name="sub").__aiter__()
    await anext(events)

    with pytest.raises(ClientInvalidOperation, match="Already subscribed"):
        await anext(client.managed_subscribe(developer_name="sub").__aiter__())

    await events.aclose()


@pytest.mark.asyncio
async def test_an_abandoned_subscription_does_not_deregister_a_newer_one(client):
    """The registry is keyed by name, so entries are released by identity

    An abandoned generator only runs its cleanup when it is closed, which can
    happen after the same topic has been subscribed to again.
    """
    client.stub.Subscribe = call_sequence(
        CallStub([fetch_response([consumer_event(b"\x01")])]),
        CallStub([fetch_response([consumer_event(b"\x02")])]),
    )

    abandoned = client.subscribe("/event/X__e")
    await anext(abandoned)
    client.unsubscribe("/event/X__e")

    current = client.subscribe("/event/X__e")
    await anext(current)
    await abandoned.aclose()

    assert client.subscriptions == frozenset({"/event/X__e"})
    assert client.unsubscribe("/event/X__e") is True

    await current.aclose()


def test_stream_slot_cancel_before_a_stream_is_open():
    """A slot is reserved before the stream exists, so cancelling is safe"""
    slot = _StreamSlot()

    slot.cancel()

    slot.call = MagicMock()
    slot.cancel()
    slot.call.cancel.assert_called_once_with()


@pytest.mark.asyncio
async def test_managed_subscribe_reauthenticates_and_resumes(client):
    expired = CallStub(
        [pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([pb2.ManagedFetchResponse(events=[consumer_event(b"\x02")])])
    client.stub.ManagedSubscribe = call_sequence(expired, resumed)

    events = [event async for event in client.managed_subscribe(developer_name="sub")]

    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    assert client.authenticator.authenticate_calls == 1
    assert resumed.requests[0].developer_name == "sub"


@pytest.mark.asyncio
async def test_managed_subscribe_gives_up_after_repeated_auth_failures(client):
    client.auth_retries = 1
    client.stub.ManagedSubscribe = lambda *args, **kwargs: CallStub(
        [], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)
    )(*args, **kwargs)

    with pytest.raises(AuthenticationError, match="1 consecutive"):
        async for _ in client.managed_subscribe(developer_name="sub"):
            pass  # pragma: no cover

    assert client.authenticator.authenticate_calls == 1


@pytest.mark.asyncio
async def test_managed_subscribe_keeps_queued_commits_across_a_restart(client):
    expired = CallStub(
        [pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")])],
        error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
    )
    resumed = CallStub([])
    client.stub.ManagedSubscribe = call_sequence(expired, resumed)
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL

    subscription = client.managed_subscribe(developer_name="sub")
    async for event in subscription:
        await subscription.commit(event["replay_id"])

    # the commit was queued on the stream that died, and rides the new one
    assert resumed.requests[1].commit_replay_id_request.replay_id == b"\x01"


@pytest.mark.asyncio
async def test_publish_raises_on_a_rejected_record(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    client.stub.Publish = AsyncMock(
        return_value=pb2.PublishResponse(
            results=[
                pb2.PublishResult(replay_id=b"\x01"),
                pb2.PublishResult(
                    error=pb2.Error(code=pb2.PUBLISH, msg="storage limit")
                ),
            ]
        )
    )

    with pytest.raises(PublishError, match="1 of 2 records rejected") as raised:
        await client.publish("/event/X__e", [{}, {}])

    # the accepted record's replay id must survive the failure
    assert raised.value.response.results[0].replay_id == b"\x01"
    assert [error.msg for error in raised.value.errors] == ["storage limit"]
    assert "PUBLISH: storage limit" in str(raised.value)


@pytest.mark.asyncio
async def test_publish_can_leave_rejected_records_to_the_caller(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    client.stub.Publish = AsyncMock(
        return_value=pb2.PublishResponse(
            results=[pb2.PublishResult(error=pb2.Error(code=pb2.PUBLISH, msg="nope"))]
        )
    )

    response = await client.publish("/event/X__e", [{}], raise_on_error=False)

    assert response.results[0].error.msg == "nope"


def test_raise_for_results_accepts_a_clean_response():
    response = pb2.PublishResponse(results=[pb2.PublishResult(replay_id=b"\x01")])

    assert SalesforcePubSubClient.raise_for_results(response) is None


@pytest.mark.asyncio
async def test_publish_stream_does_not_raise_on_rejected_records(client):
    client.stub.GetTopic = AsyncMock(
        return_value=pb2.TopicInfo(topic_name="/event/X__e", schema_id="schema-1")
    )
    rejected = pb2.PublishResponse(
        results=[pb2.PublishResult(error=pb2.Error(code=pb2.PUBLISH, msg="nope"))]
    )
    client.stub.PublishStream = CallStub(
        [rejected, pb2.PublishResponse(results=[pb2.PublishResult(replay_id=b"\x02")])],
        end_with="close",
    )

    async def batches():
        yield [{}]
        yield [{}]

    responses = [
        response async for response in client.publish_stream("/event/X__e", batches())
    ]

    # a bad batch must not tear down a stream meant to stay open
    assert len(responses) == 2
    with pytest.raises(PublishError):
        SalesforcePubSubClient.raise_for_results(responses[0])


@pytest.mark.asyncio
async def test_managed_commits_are_acknowledged_and_cleared(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    subscription = client.managed_subscribe(developer_name="sub")
    subscription.pending_commits["fixed-id"] = b"\x01"
    client.stub.ManagedSubscribe = CallStub(
        [
            pb2.ManagedFetchResponse(
                commit_response=pb2.CommitReplayResponse(
                    commit_request_id="fixed-id", replay_id=b"\x01"
                )
            )
        ]
    )

    async for _ in subscription:  # pragma: no cover - no events
        pass

    assert subscription.pending_commits == {}
    assert subscription.commit_responses["fixed-id"].replay_id == b"\x01"


@pytest.mark.asyncio
async def test_one_acknowledgement_clears_the_commits_it_batched(client):
    """The server answers a batch of commits once, naming only the last

    Clearing just the named id would leak every earlier commit, growing
    pending_commits without bound and resending them on every restart.
    """
    subscription = client.managed_subscribe(developer_name="sub")
    subscription.pending_commits.update(
        {"first": b"\x01", "second": b"\x02", "third": b"\x03"}
    )
    client.stub.ManagedSubscribe = CallStub(
        [
            pb2.ManagedFetchResponse(
                commit_response=pb2.CommitReplayResponse(
                    commit_request_id="second", replay_id=b"\x02"
                )
            )
        ]
    )

    async for _ in subscription:  # pragma: no cover - no events
        pass

    # "third" was submitted after the acknowledged commit, so it still stands
    assert subscription.pending_commits == {"third": b"\x03"}


@pytest.mark.asyncio
async def test_a_failed_commit_clears_nothing(client):
    """ErrorCode.COMMIT is unrecoverable, so the position stays queued"""
    subscription = client.managed_subscribe(developer_name="sub")
    subscription.pending_commits["first"] = b"\x01"
    client.stub.ManagedSubscribe = CallStub(
        [
            pb2.ManagedFetchResponse(
                commit_response=pb2.CommitReplayResponse(
                    commit_request_id="first",
                    error=pb2.Error(code=pb2.COMMIT, msg="unrecoverable"),
                )
            )
        ]
    )

    async for _ in subscription:  # pragma: no cover - no events
        pass

    assert subscription.pending_commits == {"first": b"\x01"}
    assert subscription.commit_responses["first"].error.msg == "unrecoverable"


@pytest.mark.asyncio
async def test_an_unknown_acknowledgement_clears_nothing(client):
    subscription = client.managed_subscribe(developer_name="sub")
    subscription.pending_commits["first"] = b"\x01"
    client.stub.ManagedSubscribe = CallStub(
        [
            pb2.ManagedFetchResponse(
                commit_response=pb2.CommitReplayResponse(commit_request_id="other")
            )
        ]
    )

    async for _ in subscription:  # pragma: no cover - no events
        pass

    assert subscription.pending_commits == {"first": b"\x01"}


def test_backoff_delay_doubles_and_is_capped():
    client = SalesforcePubSubClient(
        AuthenticatorStub(), retry_backoff=2.0, retry_backoff_max=10.0
    )

    with patch("aiosfpubsub.client.random.uniform", lambda _, high: high):
        ceilings = [client.backoff_delay(attempt) for attempt in range(1, 6)]

    assert ceilings == [2.0, 4.0, 8.0, 10.0, 10.0]


def test_backoff_delay_is_jittered():
    client = SalesforcePubSubClient(AuthenticatorStub(), retry_backoff=4.0)
    seen = {client.backoff_delay(3) for _ in range(50)}

    assert len(seen) > 1
    assert all(0.0 <= delay <= 16.0 for delay in seen)


@pytest.mark.asyncio
async def test_wait_before_retry_sleeps_for_the_backoff(client):
    client.backoff_delay = lambda attempt: 1.5 * attempt

    with patch("aiosfpubsub.client.asyncio.sleep", new=AsyncMock()) as sleep:
        await client._wait_before_retry(2)

    sleep.assert_awaited_once_with(3.0)


@pytest.mark.asyncio
async def test_subscribe_reconnects_when_the_server_closes_the_stream(client):
    """A Subscribe stream is long lived; a clean end is not a normal end"""
    closed = CallStub([fetch_response([consumer_event(b"\x01")])], end_with="close")
    reopened = CallStub([fetch_response([consumer_event(b"\x02")])])
    client.stub.Subscribe = call_sequence(closed, reopened)

    events = [event async for event in client.subscribe("/event/X__e")]

    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    # the reopened stream resumes after the last event it had delivered
    assert reopened.requests[0].replay_preset == pb2.CUSTOM
    assert reopened.requests[0].replay_id == b"\x01"


@pytest.mark.asyncio
async def test_a_productive_stream_reconnects_without_waiting(client):
    delays = []
    client._wait_before_retry = AsyncMock(side_effect=lambda a: delays.append(a))
    client.stub.Subscribe = call_sequence(
        CallStub([fetch_response([consumer_event(b"\x01")])], end_with="close"),
        CallStub([fetch_response([consumer_event(b"\x02")])]),
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert delays == []


@pytest.mark.asyncio
async def test_an_unproductive_stream_backs_off_before_reconnecting(client):
    delays = []
    client._wait_before_retry = AsyncMock(side_effect=lambda a: delays.append(a))
    client.stub.Subscribe = call_sequence(
        CallStub([], end_with="close"),
        CallStub([], end_with="close"),
        CallStub([fetch_response([consumer_event(b"\x01")])]),
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    # the attempt number grows, which is what widens the backoff
    assert delays == [1, 2]


@pytest.mark.asyncio
async def test_subscribe_gives_up_after_unproductive_reconnects(client):
    """Bounded, so a permanently broken subscription cannot loop forever"""
    client.reconnect_retries = 2
    client.stub.Subscribe = lambda *args, **kwargs: CallStub([], end_with="close")(
        *args, **kwargs
    )

    with pytest.raises(ClientError, match="closed by the server 2 times"):
        async for _ in client.subscribe("/event/X__e"):  # pragma: no cover
            pass


@pytest.mark.asyncio
async def test_delivering_an_event_resets_the_reconnect_count(client):
    client.reconnect_retries = 1
    client.stub.Subscribe = call_sequence(
        CallStub([], end_with="close"),
        CallStub([fetch_response([consumer_event(b"\x01")])], end_with="close"),
        CallStub([], end_with="close"),
        CallStub([fetch_response([consumer_event(b"\x02")])]),
    )

    events = [event async for event in client.subscribe("/event/X__e")]

    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]


@pytest.mark.asyncio
async def test_reauthentication_backs_off(client):
    delays = []
    client._wait_before_retry = AsyncMock(side_effect=lambda a: delays.append(a))
    client.stub.Subscribe = call_sequence(
        CallStub([], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)),
        CallStub([fetch_response([consumer_event(b"\x01")])]),
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert delays == [1]


@pytest.mark.asyncio
async def test_managed_subscribe_reconnects_when_the_server_closes_the_stream(client):
    closed = CallStub(
        [pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")])],
        end_with="close",
    )
    reopened = CallStub([pb2.ManagedFetchResponse(events=[consumer_event(b"\x02")])])
    client.stub.ManagedSubscribe = call_sequence(closed, reopened)

    events = [event async for event in client.managed_subscribe(developer_name="sub")]

    assert [event["replay_id"] for event in events] == [b"\x01", b"\x02"]
    assert reopened.requests[0].developer_name == "sub"


@pytest.mark.asyncio
async def test_managed_reconnect_resends_unacknowledged_commits(client):
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    closed = CallStub(
        [pb2.ManagedFetchResponse(events=[consumer_event(b"\x01")])],
        end_with="close",
    )
    reopened = CallStub([])
    client.stub.ManagedSubscribe = call_sequence(closed, reopened)

    subscription = client.managed_subscribe(developer_name="sub")
    async for event in subscription:
        await subscription.commit(event["replay_id"])

    assert reopened.requests[1].commit_replay_id_request.replay_id == b"\x01"


@pytest.mark.asyncio
async def test_managed_subscribe_gives_up_after_unproductive_reconnects(client):
    client.reconnect_retries = 1
    client.stub.ManagedSubscribe = lambda *args, **kwargs: CallStub(
        [], end_with="close"
    )(*args, **kwargs)

    with pytest.raises(ClientError, match="closed by the server 1 times"):
        async for _ in client.managed_subscribe(developer_name="sub"):
            pass  # pragma: no cover


@pytest.mark.asyncio
async def test_backoff_precedes_the_token_request(client):
    """A rejection re-authentication can't fix must not hammer the endpoint"""
    order = []
    client._wait_before_retry = AsyncMock(side_effect=lambda a: order.append("wait"))
    client.authenticator.authenticate = AsyncMock(
        side_effect=lambda: order.append("authenticate")
    )
    client.stub.Subscribe = call_sequence(
        CallStub([], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)),
        CallStub([], error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED)),
        CallStub([fetch_response([consumer_event(b"\x01")])]),
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert order == ["wait", "authenticate", "wait", "authenticate"]


@pytest.mark.asyncio
async def test_a_productive_stream_reauthenticates_without_waiting(client):
    delays = []
    client._wait_before_retry = AsyncMock(side_effect=lambda a: delays.append(a))
    client.stub.Subscribe = call_sequence(
        CallStub(
            [fetch_response([consumer_event(b"\x01")])],
            error=RpcErrorStub(grpc.StatusCode.UNAUTHENTICATED),
        ),
        CallStub([fetch_response([consumer_event(b"\x02")])]),
    )

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert delays == []


@pytest.mark.asyncio
async def test_a_keepalive_anchors_a_stream_that_is_then_closed(client):
    """A reconnect must not silently skip what was published while waiting

    The stream delivered no event, so there is no marker to resume from, but
    the keepalive named a concrete position and the reconnect uses it instead
    of asking for NEW_EVENTS all over again.
    """
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    closed = CallStub([fetch_response(latest_replay_id=b"\x0a")], end_with="close")
    reopened = CallStub([])
    client.stub.Subscribe = call_sequence(closed, reopened)

    async for _ in client.subscribe("/event/X__e"):  # pragma: no cover - no events
        pass

    assert closed.requests[0].replay_preset == pb2.LATEST
    assert reopened.requests[0].replay_preset == pb2.CUSTOM
    assert reopened.requests[0].replay_id == b"\x0a"


@pytest.mark.asyncio
async def test_a_stream_closed_before_any_response_cannot_anchor(client):
    """A known limitation, asserted so a change to it is deliberate

    The stream produced nothing at all, so no position is known and, with an
    empty replay storage, the reconnect can only ask for NEW_EVENTS again.
    Anything published in between is skipped. A storing replay storage or
    ALL_EVENTS avoids it.
    """
    client.replay_storage_policy = ReplayMarkerStoragePolicy.MANUAL
    reopened = CallStub([fetch_response([consumer_event(b"\x01")])])
    client.stub.Subscribe = call_sequence(CallStub([], end_with="close"), reopened)

    async for _ in client.subscribe("/event/X__e"):
        pass

    assert reopened.requests[0].replay_preset == pb2.LATEST


CHANGE_EVENT_SCHEMA = {
    "type": "record",
    "name": "AccountChangeEvent",
    "fields": [
        {
            "name": "ChangeEventHeader",
            "type": {
                "type": "record",
                "name": "ChangeEventHeader",
                "fields": [
                    {
                        "name": "changedFields",
                        "type": {"type": "array", "items": "string"},
                    }
                ],
            },
        },
        {"name": "Name", "type": ["null", "string"]},
    ],
}


def change_event(client, schema_id="schema-cdc"):
    """Return a ConsumerEvent whose header marks the Name field as changed"""
    schema = fastavro.parse_schema(CHANGE_EVENT_SCHEMA)
    client._schema_cache[schema_id] = schema
    payload = io.BytesIO()
    fastavro.schemaless_writer(
        payload,
        schema,
        {"ChangeEventHeader": {"changedFields": ["0x02"]}, "Name": None},
    )
    return pb2.ConsumerEvent(
        event=pb2.ProducerEvent(
            id="evt", schema_id=schema_id, payload=payload.getvalue()
        ),
        replay_id=b"\x01",
    )


@pytest.mark.asyncio
async def test_change_event_bitmaps_are_left_alone_by_default(client):
    client.stub.Subscribe = CallStub([fetch_response([change_event(client)])])

    event = await anext(client.subscribe("/data/AccountChangeEvent"))

    assert event["payload"]["ChangeEventHeader"]["changedFields"] == ["0x02"]


@pytest.mark.asyncio
async def test_change_event_bitmaps_are_expanded_when_asked(client):
    client.expand_change_event_header = True
    client.stub.Subscribe = CallStub([fetch_response([change_event(client)])])

    event = await anext(client.subscribe("/data/AccountChangeEvent"))

    assert event["payload"]["ChangeEventHeader"]["changedFields"] == ["Name"]


@pytest.mark.asyncio
async def test_expanding_leaves_a_platform_event_untouched(client):
    """The option is safe to leave on for a client subscribed to both"""
    client.expand_change_event_header = True
    client.stub.Subscribe = CallStub([fetch_response([consumer_event(b"\x01")])])

    event = await anext(client.subscribe("/event/X__e"))

    assert event["payload"] == {}
