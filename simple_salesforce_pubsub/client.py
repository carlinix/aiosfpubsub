"""Client class implementation"""

import asyncio
import io
import json
import logging
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Iterable
from enum import Enum, auto, unique
from types import TracebackType
from typing import Any

import fastavro
import grpc

from . import pubsub_api_pb2 as pb2
from . import pubsub_api_pb2_grpc as pb2_grpc
from .auth import AuthenticatorBase
from .exceptions import (
    AuthenticationError,
    ClientError,
    ClientInvalidOperation,
    SchemaError,
)
from .replay import (
    FetchPosition,
    ReplayMarkerStorage,
    ReplayOption,
    ReplayParameter,
    create_replay_storage,
)

DEFAULT_ENDPOINT = "api.pubsub.salesforce.com:7443"
DEFAULT_NUM_REQUESTED = 10
DEFAULT_AUTH_RETRIES = 3
LOGGER = logging.getLogger(__name__)

#: A decoded event, as yielded by the subscription iterators
Event = dict[str, Any]


@unique
class ReplayMarkerStoragePolicy(Enum):
    """Defines the available replay marker storage policies"""

    #: Store the replay marker of events automatically, as soon as they're
    #: consumed.
    #: The downside of this approach is that the replay marker of an event
    #: will be stored (thus marking it as successfully consumed) even if the
    #: processing of the event fails in the client side code
    AUTOMATIC = auto()
    #: Store the replay marker of events manually, after the event has been
    #: successfully processed in client side code, with
    #: :meth:`SalesforcePubSubClient.commit_replay` or
    #: :meth:`ManagedSubscription.commit`
    MANUAL = auto()


class _AuthenticationExpired(Exception):
    """Internal signal that the server rejected the call metadata and that the
    subscription should be re-established with a fresh access token"""


class _ReplayIdRejected(Exception):
    """Internal signal that the server rejected the replay id the
    subscription started from"""


class _SubscriptionCancelled(Exception):
    """Internal signal that the subscription was cancelled locally, by
    :meth:`SalesforcePubSubClient.unsubscribe` or by closing the client"""


class _StreamSlot:
    """The registry entry for one active stream

    Held per subscription rather than per topic, so that the cleanup of an
    abandoned subscription can't deregister a newer one that has taken over
    the same name. The call is attached once the stream is opened, which is
    later than the slot is reserved.
    """

    def __init__(self) -> None:
        self.call: Any = None

    def cancel(self) -> None:
        """Cancel the stream, if one is open"""
        if self.call is not None:
            self.call.cancel()


class _ResumePosition:
    """Tracks the position a subscription should be re-established from

    The client's :obj:`~.ReplayMarkerStorage` is the source of truth, but it
    is legitimately empty in two cases: :obj:`~.ConstantReplayId` never stores
    anything, and under :obj:`ReplayMarkerStoragePolicy.MANUAL` nothing is
    stored until the consumer commits. Recomputing the fetch position then
    would restart the subscription from the default replay option, silently
    dropping everything published since it began, so the position the stream
    started from is captured once and reused.
    """

    def __init__(self, initial: FetchPosition) -> None:
        #: The position the subscription originally started from
        self.initial = initial
        #: The last position known to be safe to resume from, for storages
        #: which don't persist markers
        self.resumable: bytes | None = None
        #: Whether any event has been delivered to the consumer yet
        self.delivered = False

    def anchor(self, replay_id: bytes) -> None:
        """Pin a "from now" subscription to the concrete *replay_id*

        :obj:`~.ReplayOption.NEW_EVENTS` means "whatever is published from
        now on", so re-establishing a subscription with it skips everything
        published in between. A keepalive received before any event names the
        position the stream actually started at, which can be resumed from
        exactly.

        :param replay_id: The ``latest_replay_id`` of a keepalive response
        """
        if not self.delivered and self.initial[0] == pb2.LATEST:
            self.initial = (pb2.CUSTOM, replay_id)

    def advance(self, replay_id: bytes) -> None:
        """Record *replay_id* as consumed

        Only called under :obj:`ReplayMarkerStoragePolicy.AUTOMATIC`; under
        the manual policy an event is not consumed until it is committed.
        """
        self.resumable = replay_id

    def restart(self, initial: FetchPosition) -> None:
        """Discard what is known and start over from *initial*"""
        self.initial = initial
        self.resumable = None
        self.delivered = False

    def resolve(self, stored_marker: bytes | None) -> FetchPosition:
        """Return the position to open the next stream with

        :param stored_marker: The marker held by the replay storage, if any
        """
        if stored_marker:
            return pb2.CUSTOM, stored_marker
        if self.resumable:
            return pb2.CUSTOM, self.resumable
        return self.initial


class SalesforcePubSubClient:
    """Salesforce Pub/Sub API client"""

    def __init__(
        self,
        authenticator: AuthenticatorBase,
        *,
        endpoint: str = DEFAULT_ENDPOINT,
        replay: ReplayParameter = ReplayOption.NEW_EVENTS,
        replay_storage_policy: ReplayMarkerStoragePolicy = (
            ReplayMarkerStoragePolicy.AUTOMATIC
        ),
        replay_fallback: ReplayOption | None = None,
        num_requested: int = DEFAULT_NUM_REQUESTED,
        auth_retries: int = DEFAULT_AUTH_RETRIES,
    ) -> None:
        """
        :param authenticator: An authenticator object
        :param endpoint: Host and port of the Pub/Sub API endpoint
        :param replay: A :obj:`~.ReplayOption` or an object capable of \
        storing replay ids. You can use one of the \
        :obj:`ReplayOptions <.ReplayOption>`, an object supporting the \
        MutableMapping protocol like :obj:`dict`, \
        :obj:`~collections.defaultdict`, :obj:`~shelve.Shelf` etc. or a \
        custom :obj:`~.ReplayMarkerStorage` implementation. \
        Ignored by :meth:`managed_subscribe`, where Salesforce tracks the \
        replay position instead.
        :param replay_storage_policy: Defines at which point the replay \
        marker of consumed events will be stored
        :param replay_fallback: Replay option to fall back on when a \
        subscription is rejected because the replay id it started from is \
        no longer valid, typically because it fell outside the event \
        retention window. Without it, such a rejection is raised as a \
        :obj:`~.ClientError`.
        :param num_requested: The number of events to request from the \
        server at a time. The subscription iterators replenish this budget \
        as events are consumed, which is what applies backpressure.
        :param auth_retries: How many times in a row :meth:`subscribe` may \
        re-authenticate and resume before giving up. The count is reset \
        whenever a resumed subscription delivers an event, so it only bounds \
        failures the re-authentication can't fix, such as revoked \
        credentials.
        :raise TypeError: If *authenticator* or *replay* is of an \
        unsupported type
        """
        if not isinstance(authenticator, AuthenticatorBase):
            raise TypeError(
                f"authenticator should be an instance of {AuthenticatorBase.__name__}."
            )
        replay_storage = create_replay_storage(replay)
        if replay_storage is None:
            raise TypeError(
                f"{type(replay).__name__!r} is not a valid type for the replay "
                "parameter."
            )
        #: An authenticator object
        self.authenticator = authenticator
        #: Host and port of the Pub/Sub API endpoint
        self.endpoint = endpoint
        #: :obj:`~.ReplayMarkerStorage` instance capable of storing replay ids
        self.replay_storage: ReplayMarkerStorage = replay_storage
        #: Defines at which point replay markers of consumed events are stored
        self.replay_storage_policy = replay_storage_policy
        #: Replay option to fall back on when a replay id gets rejected
        self.replay_fallback = replay_fallback
        #: The number of events requested from the server at a time
        self.num_requested = num_requested
        #: Consecutive re-authentication attempts allowed before giving up
        self.auth_retries = auth_retries
        self.channel: grpc.aio.Channel | None = None
        self.stub: pb2_grpc.PubSubStub | None = None
        self._schema_cache: dict[str, Any] = {}
        self._streams: dict[str, _StreamSlot] = {}

    async def open(self) -> None:
        """Authenticate and establish a connection with the Pub/Sub API

        :raise AuthenticationError: If the server rejects the authentication \
        request or if a network failure occurs during the authentication
        """
        await self.authenticator.authenticate()
        LOGGER.info(
            "Successful authentication. Instance URL: %r.",
            self.authenticator.instance_url,
        )
        credentials = grpc.ssl_channel_credentials()
        self.channel = grpc.aio.secure_channel(self.endpoint, credentials)
        self.stub = pb2_grpc.PubSubStub(self.channel)
        LOGGER.info("Connected to the Pub/Sub API at %r.", self.endpoint)

    #: Deprecated alias of :meth:`open`, kept for the original 0.1.0 surface
    connect = open

    async def close(self) -> None:
        """Close the connection with the Pub/Sub API

        Every active subscription is cancelled, so the consumers iterating
        them stop.
        """
        for name in list(self._streams):
            self.unsubscribe(name)
        if self.channel:
            await self.channel.close()
            self.channel = None
            self.stub = None

    async def __aenter__(self) -> "SalesforcePubSubClient":
        await self.open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    def _get_stub(self) -> pb2_grpc.PubSubStub:
        """Return the stub, checking that the client is open

        :raise ClientInvalidOperation: If the client is not open
        """
        if self.stub is None:
            raise ClientInvalidOperation("The client is closed. Call open() first.")
        return self.stub

    def _get_metadata(self) -> tuple[tuple[str, str], ...]:
        """Return the call metadata needed for authentication"""
        return self.authenticator.get_grpc_metadata()

    async def get_topic_info(self, topic_name: str) -> pb2.TopicInfo:
        """Fetch information about the given *topic_name*

        :raise ClientInvalidOperation: If the client is not open
        :raise ClientError: If the request gets rejected by the server
        """
        stub = self._get_stub()
        request = pb2.TopicRequest(topic_name=topic_name)
        try:
            return await stub.GetTopic(request, metadata=self._get_metadata())
        except grpc.aio.AioRpcError as error:
            raise ClientError(f"Failed to get topic info: {error.details()}") from error

    async def get_schema(self, schema_id: str) -> Any:
        """Fetch and parse the Avro schema with the given *schema_id*

        Parsed schemas are cached by schema id for the lifetime of the client.

        :raise ClientInvalidOperation: If the client is not open
        :raise SchemaError: If the schema can't be fetched or parsed
        """
        stub = self._get_stub()
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]

        request = pb2.SchemaRequest(schema_id=schema_id)
        try:
            response = await stub.GetSchema(request, metadata=self._get_metadata())
        except grpc.aio.AioRpcError as error:
            raise SchemaError(f"Failed to fetch schema: {error.details()}") from error
        try:
            parsed_schema = fastavro.parse_schema(json.loads(response.schema_json))
        except Exception as error:
            raise SchemaError(f"Failed to parse schema: {error}") from error
        self._schema_cache[schema_id] = parsed_schema
        return parsed_schema

    def _decode_payload(self, schema: Any, payload: bytes) -> dict[str, Any]:
        """Decode an Avro *payload* using the given *schema*"""
        return fastavro.schemaless_reader(io.BytesIO(payload), schema)

    def _encode_payload(self, schema: Any, record: dict[str, Any]) -> bytes:
        """Encode a *record* into an Avro payload using the given *schema*"""
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema, record)
        return buffer.getvalue()

    async def _decode_event(self, consumer_event: pb2.ConsumerEvent) -> Event:
        """Decode a *consumer_event* into the mapping yielded to the consumer

        :raise SchemaError: If the schema can't be fetched or parsed
        """
        schema_id = consumer_event.event.schema_id
        schema = await self.get_schema(schema_id)
        return {
            "replay_id": consumer_event.replay_id,
            "id": consumer_event.event.id,
            "schema_id": schema_id,
            "headers": {
                header.key: header.value for header in consumer_event.event.headers
            },
            "payload": self._decode_payload(schema, consumer_event.event.payload),
        }

    async def commit_replay(self, topic_name: str, replay_id: bytes) -> None:
        """Store the *replay_id* of a successfully processed event

        Call this under :obj:`ReplayMarkerStoragePolicy.MANUAL` to advance the
        stored position of a :meth:`subscribe` subscription. Under
        :obj:`~ReplayMarkerStoragePolicy.AUTOMATIC` the subscription advances
        the marker itself.

        :param topic_name: Name of the subscribed topic
        :param replay_id: The ``replay_id`` of the processed event
        """
        await self.replay_storage.set_replay_marker(topic_name, replay_id)

    async def subscribe(
        self,
        topic_name: str,
        *,
        num_requested: int | None = None,
        replay_fallback: ReplayOption | None = None,
    ) -> AsyncGenerator[Event, None]:
        """Subscribe to *topic_name* and yield decoded events

        The replay position is tracked client side, in the client's
        :obj:`~.ReplayMarkerStorage`. Use :meth:`managed_subscribe` to let
        Salesforce track it instead.

        If the server rejects the call metadata because the access token
        expired, the authenticator is invoked again and the subscription is
        re-established. It resumes from the stored replay marker, or, when the
        storage holds none, from the position the subscription originally
        started at, so events are not dropped by a policy or a storage which
        has nothing recorded yet.

        Iteration ends when :meth:`unsubscribe` is called for *topic_name*, or
        when the client is closed.

        :param topic_name: Name of the topic, such as \
        ``/event/My_Event__e`` or ``/data/AccountChangeEvent``
        :param num_requested: Overrides the client's ``num_requested``
        :param replay_fallback: Overrides the client's ``replay_fallback``
        :raise ClientInvalidOperation: If the client is not open, or if \
        *topic_name* is already subscribed to
        :raise AuthenticationError: If re-authentication fails, or if the \
        server keeps rejecting the call metadata after ``auth_retries`` \
        consecutive attempts
        :raise ClientError: If the subscription gets rejected by the server
        :raise SchemaError: If an event's schema can't be fetched or parsed
        """
        self._get_stub()
        if topic_name in self._streams:
            raise ClientInvalidOperation(
                f"Already subscribed to {topic_name!r}. Call unsubscribe() first."
            )
        budget = num_requested if num_requested is not None else self.num_requested
        fallback = (
            replay_fallback if replay_fallback is not None else self.replay_fallback
        )
        state = _ResumePosition(
            await self.replay_storage.get_fetch_position(topic_name)
        )
        slot = _StreamSlot()
        self._streams[topic_name] = slot
        auth_refresh = ""
        attempts = 0
        fallback_used = False
        try:
            while True:
                delivered = False
                try:
                    async for event in self._subscribe_once(
                        topic_name, budget, auth_refresh, state, slot
                    ):
                        delivered = True
                        yield event
                    return
                except _SubscriptionCancelled:
                    return
                except _AuthenticationExpired as error:
                    # a subscription that delivered events was healthy, so the
                    # expiry is routine rather than a credential problem
                    attempts = 0 if delivered else attempts + 1
                    if attempts > self.auth_retries:
                        raise AuthenticationError(
                            f"The access token was rejected after "
                            f"{self.auth_retries} consecutive re-authentication "
                            f"attempts."
                        ) from error
                    LOGGER.info(
                        "Access token rejected, re-authenticating and resuming %r.",
                        topic_name,
                    )
                    await self.authenticator.authenticate()
                    auth_refresh = self.authenticator.access_token or ""
                except _ReplayIdRejected as error:
                    if fallback is None or fallback_used:
                        raise ClientError(
                            f"Subscribe failed: {error}"
                        ) from error.__cause__
                    LOGGER.warning(
                        "Subscription to %r failed with message: %s, "
                        "retrying subscription with %r.",
                        topic_name,
                        error,
                        fallback,
                    )
                    fallback_used = True
                    await self.replay_storage.clear_replay_marker(topic_name)
                    state.restart((fallback.value, b""))
        finally:
            if self._streams.get(topic_name) is slot:
                del self._streams[topic_name]

    async def _subscribe_once(
        self,
        topic_name: str,
        num_requested: int,
        auth_refresh: str,
        state: _ResumePosition,
        slot: _StreamSlot,
    ) -> AsyncGenerator[Event, None]:
        """Run a single ``Subscribe`` stream until it ends or fails

        :raise _AuthenticationExpired: If the server rejects the call metadata
        :raise _ReplayIdRejected: If the server rejects the replay id
        :raise _SubscriptionCancelled: If the subscription was cancelled \
        locally
        """
        stub = self._get_stub()
        stored_marker = await self.replay_storage.get_replay_marker(topic_name)
        replay_preset, replay_id = state.resolve(stored_marker)
        requests: asyncio.Queue[pb2.FetchRequest] = asyncio.Queue()

        async def request_generator() -> AsyncIterator[pb2.FetchRequest]:
            yield pb2.FetchRequest(
                topic_name=topic_name,
                replay_preset=replay_preset,
                replay_id=replay_id,
                num_requested=num_requested,
                auth_refresh=auth_refresh,
            )
            while True:
                yield await requests.get()

        call = stub.Subscribe(request_generator(), metadata=self._get_metadata())
        slot.call = call
        automatic = self.replay_storage_policy is ReplayMarkerStoragePolicy.AUTOMATIC
        try:
            async for response in call:
                for consumer_event in response.events:
                    state.delivered = True
                    yield await self._decode_event(consumer_event)
                    if automatic:
                        state.advance(consumer_event.replay_id)
                        await self.replay_storage.set_replay_marker(
                            topic_name, consumer_event.replay_id
                        )
                # A keepalive carries no events but a fresh latest_replay_id.
                # Advancing on it keeps an idle subscription from re-reading
                # the retention window on reconnect, but there is nothing to
                # consume, so a MANUAL policy must not advance here. It can
                # still anchor a "from now" subscription, which loses no
                # uncommitted event because none has been delivered yet.
                if not response.events and response.latest_replay_id:
                    state.anchor(response.latest_replay_id)
                    if automatic:
                        state.advance(response.latest_replay_id)
                        await self.replay_storage.set_replay_marker(
                            topic_name, response.latest_replay_id
                        )
                if response.pending_num_requested <= 0:
                    await requests.put(
                        pb2.FetchRequest(
                            topic_name=topic_name, num_requested=num_requested
                        )
                    )
        except grpc.aio.AioRpcError as error:
            code = error.code()
            if code is grpc.StatusCode.UNAUTHENTICATED:
                raise _AuthenticationExpired from error
            if code is grpc.StatusCode.CANCELLED:
                raise _SubscriptionCancelled from error
            if self.is_replay_id_error(error):
                raise _ReplayIdRejected(error.details() or "") from error
            raise ClientError(f"Subscribe failed: {error.details()}") from error

    @staticmethod
    def is_replay_id_error(error: grpc.aio.AioRpcError) -> bool:
        """Return whether *error* means the replay id was rejected

        The usual cause is a replay id that has fallen outside the event
        retention window. Unlike the Streaming API, the Pub/Sub API has no
        error code for this: the ``ErrorCode`` enum of the protocol only
        covers publish and commit failures, so the condition has to be
        recognised from the gRPC status. Override this in a subclass if the
        server wording changes.

        :param error: The error raised by the ``Subscribe`` stream
        """
        if error.code() is not grpc.StatusCode.INVALID_ARGUMENT:
            return False
        return "replay" in (error.details() or "").lower()

    def unsubscribe(self, name: str) -> bool:
        """Stop the subscription registered under *name*

        The generator being iterated stops, so its consumer's ``async for``
        loop ends normally.

        :param name: A topic name for a :meth:`subscribe` stream, or the \
        subscription id or developer name of a :meth:`managed_subscribe` one
        :return: Whether there was a subscription to stop
        """
        slot = self._streams.pop(name, None)
        if slot is None:
            return False
        slot.cancel()
        return True

    @property
    def subscriptions(self) -> frozenset[str]:
        """Names under which subscriptions are currently registered"""
        return frozenset(self._streams)

    def managed_subscribe(
        self,
        *,
        subscription_id: str = "",
        developer_name: str = "",
        num_requested: int | None = None,
    ) -> "ManagedSubscription":
        """Subscribe to a Managed Event Subscription configured in the org

        Salesforce tracks the replay position server side, so the client's
        :obj:`~.ReplayMarkerStorage` is not consulted. Under
        :obj:`ReplayMarkerStoragePolicy.AUTOMATIC` the returned subscription
        commits each consumed event's replay id; under
        :obj:`~ReplayMarkerStoragePolicy.MANUAL` call
        :meth:`ManagedSubscription.commit`.

        :param subscription_id: Id of the managed event subscription
        :param developer_name: Developer name of the managed event \
        subscription. Supply this or *subscription_id*.
        :param num_requested: Overrides the client's ``num_requested``
        :raise ClientInvalidOperation: If the client is not open
        :raise ValueError: If neither *subscription_id* nor *developer_name* \
        is given
        """
        self._get_stub()
        if not subscription_id and not developer_name:
            raise ValueError(
                "either subscription_id or developer_name must be provided."
            )
        return ManagedSubscription(
            self,
            subscription_id=subscription_id,
            developer_name=developer_name,
            num_requested=(
                num_requested if num_requested is not None else self.num_requested
            ),
        )

    async def publish(
        self, topic_name: str, records: list[dict[str, Any]]
    ) -> pb2.PublishResponse:
        """Publish *records* to *topic_name*

        :param topic_name: Name of the topic to publish to
        :param records: Event records, encoded with the topic's Avro schema
        :raise ClientInvalidOperation: If the client is not open
        :raise ClientError: If the publish request gets rejected by the server
        :raise SchemaError: If the topic's schema can't be fetched or parsed
        """
        stub = self._get_stub()
        topic_info = await self.get_topic_info(topic_name)
        schema_id = topic_info.schema_id
        schema = await self.get_schema(schema_id)

        events = [
            pb2.ProducerEvent(
                schema_id=schema_id, payload=self._encode_payload(schema, record)
            )
            for record in records
        ]
        request = pb2.PublishRequest(topic_name=topic_name, events=events)
        try:
            return await stub.Publish(request, metadata=self._get_metadata())
        except grpc.aio.AioRpcError as error:
            raise ClientError(f"Publish failed: {error.details()}") from error

    async def publish_stream(
        self,
        topic_name: str,
        batches: AsyncIterable[Iterable[dict[str, Any]]],
    ) -> AsyncGenerator[pb2.PublishResponse, None]:
        """Publish successive *batches* of records over a single stream

        Unlike :meth:`publish`, which pays a round trip per call, this keeps
        one ``PublishStream`` open and yields the server's response for each
        batch as it arrives. The topic's schema is fetched once.

        :param topic_name: Name of the topic to publish to
        :param batches: An asynchronous iterable of record batches
        :raise ClientInvalidOperation: If the client is not open
        :raise ClientError: If the stream gets rejected by the server
        :raise SchemaError: If the topic's schema can't be fetched or parsed
        """
        stub = self._get_stub()
        topic_info = await self.get_topic_info(topic_name)
        schema_id = topic_info.schema_id
        schema = await self.get_schema(schema_id)

        async def request_generator() -> AsyncIterator[pb2.PublishRequest]:
            async for records in batches:
                yield pb2.PublishRequest(
                    topic_name=topic_name,
                    events=[
                        pb2.ProducerEvent(
                            schema_id=schema_id,
                            payload=self._encode_payload(schema, record),
                        )
                        for record in records
                    ],
                )

        try:
            async for response in stub.PublishStream(
                request_generator(), metadata=self._get_metadata()
            ):
                yield response
        except grpc.aio.AioRpcError as error:
            raise ClientError(f"Publish stream failed: {error.details()}") from error


class ManagedSubscription:
    """A ``ManagedSubscribe`` stream, with the replay position tracked by
    Salesforce

    Instances are created by
    :meth:`SalesforcePubSubClient.managed_subscribe`, not directly.
    """

    def __init__(
        self,
        client: SalesforcePubSubClient,
        *,
        subscription_id: str = "",
        developer_name: str = "",
        num_requested: int = DEFAULT_NUM_REQUESTED,
    ) -> None:
        self.client = client
        #: Id of the managed event subscription
        self.subscription_id = subscription_id
        #: Developer name of the managed event subscription
        self.developer_name = developer_name
        #: The number of events requested from the server at a time
        self.num_requested = num_requested
        #: The name this subscription is registered under on the client
        self.name = subscription_id or developer_name
        self._requests: asyncio.Queue[pb2.ManagedFetchRequest] = asyncio.Queue()
        self._slot = _StreamSlot()
        #: Commit responses received from the server, keyed by request id
        self.commit_responses: dict[str, pb2.CommitReplayResponse] = {}

    def __repr__(self) -> str:
        """Formal string representation"""
        cls_name = type(self).__name__
        return (
            f"{cls_name}(subscription_id={self.subscription_id!r}, "
            f"developer_name={self.developer_name!r})"
        )

    def cancel(self) -> bool:
        """Stop the subscription

        Equivalent to calling :meth:`SalesforcePubSubClient.unsubscribe` with
        this subscription's :obj:`name`.

        :return: Whether there was a stream to stop
        """
        return self.client.unsubscribe(self.name)

    async def commit(self, replay_id: bytes) -> str:
        """Ask Salesforce to store *replay_id* as the subscription's position

        The commit is queued onto the subscription's request stream, so it is
        only delivered while the subscription is being iterated. The server's
        acknowledgement lands in :obj:`commit_responses` under the returned id.

        :param replay_id: The ``replay_id`` of the processed event
        :return: The generated ``commit_request_id``
        """
        commit_request_id = str(uuid.uuid4())
        await self._requests.put(
            pb2.ManagedFetchRequest(
                commit_replay_id_request=pb2.CommitReplayRequest(
                    commit_request_id=commit_request_id, replay_id=replay_id
                )
            )
        )
        return commit_request_id

    async def __aiter__(self) -> AsyncGenerator[Event, None]:
        """Iterate over the decoded events of the managed subscription

        :raise ClientInvalidOperation: If the client is not open, or if a \
        subscription is already registered under this one's :obj:`name`
        :raise ClientError: If the subscription gets rejected by the server
        :raise SchemaError: If an event's schema can't be fetched or parsed
        """
        client = self.client
        stub = client._get_stub()
        if self.name in client._streams:
            raise ClientInvalidOperation(
                f"Already subscribed to {self.name!r}. Call unsubscribe() first."
            )
        client._streams[self.name] = self._slot
        num_requested = self.num_requested

        async def request_generator() -> AsyncIterator[pb2.ManagedFetchRequest]:
            yield pb2.ManagedFetchRequest(
                subscription_id=self.subscription_id,
                developer_name=self.developer_name,
                num_requested=num_requested,
            )
            while True:
                yield await self._requests.get()

        call = stub.ManagedSubscribe(
            request_generator(), metadata=client._get_metadata()
        )
        self._slot.call = call
        automatic = client.replay_storage_policy is ReplayMarkerStoragePolicy.AUTOMATIC
        try:
            async for response in call:
                if response.HasField("commit_response"):
                    commit_response = response.commit_response
                    self.commit_responses[commit_response.commit_request_id] = (
                        commit_response
                    )
                for consumer_event in response.events:
                    yield await client._decode_event(consumer_event)
                    if automatic:
                        await self.commit(consumer_event.replay_id)
                if response.pending_num_requested <= 0:
                    await self._requests.put(
                        pb2.ManagedFetchRequest(num_requested=num_requested)
                    )
        except grpc.aio.AioRpcError as error:
            if error.code() is grpc.StatusCode.CANCELLED:
                return
            raise ClientError(f"Managed subscribe failed: {error.details()}") from error
        finally:
            if client._streams.get(self.name) is self._slot:
                del client._streams[self.name]
