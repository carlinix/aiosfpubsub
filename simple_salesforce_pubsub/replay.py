"""Replay marker storage implementations

Ported from :mod:`aiosfstream.replay`. The Streaming API identified a message
position with an ordered integer replay id plus a message creation date, which
let it discard replayed messages by comparing dates. The Pub/Sub API uses an
opaque ``bytes`` replay id which is neither ordered nor comparable, and carries
no creation date outside the Avro payload, so markers here are stored
unconditionally and the staleness check has no counterpart.
"""

import reprlib
from abc import ABC, abstractmethod
from collections import abc
from collections.abc import MutableMapping
from enum import IntEnum, unique

from . import pubsub_api_pb2 as pb2

#: A ``(replay_preset, replay_id)`` pair, as accepted by
#: :obj:`~simple_salesforce_pubsub.pubsub_api_pb2.FetchRequest`
FetchPosition = tuple[int, bytes]


@unique
class ReplayOption(IntEnum):
    """Replay options supported by Salesforce

    The members keep the ``aiosfstream`` names, but their values are Pub/Sub
    API :obj:`ReplayPreset` values rather than Streaming API replay ids.
    """

    #: Receive only the events published after the subscription is established
    NEW_EVENTS = pb2.LATEST
    #: Receive all events still inside the retention window, followed by the
    #: events published after the subscription is established
    ALL_EVENTS = pb2.EARLIEST


class ReplayMarkerStorage(ABC):
    """Abstract base class for replay marker storage implementations"""

    def __init__(self, default_option: ReplayOption = ReplayOption.NEW_EVENTS) -> None:
        """
        :param default_option: The replay option to use for a subscription \
        which has no stored replay marker yet
        """
        #: The replay option to use in the absence of a stored replay marker
        self.default_option = default_option

    @abstractmethod
    async def get_replay_marker(self, topic_name: str) -> bytes | None:
        """Retrieve the stored replay marker for the given *topic_name*

        :param topic_name: Name of the subscribed topic
        :return: A replay id or ``None`` if there is nothing stored for \
        the given *topic_name*
        """

    @abstractmethod
    async def set_replay_marker(self, topic_name: str, replay_id: bytes) -> None:
        """Store the *replay_id* for the given *topic_name*

        :param topic_name: Name of the subscribed topic
        :param replay_id: An opaque replay id
        """

    @abstractmethod
    async def clear_replay_marker(self, topic_name: str) -> None:
        """Discard the stored replay marker for the given *topic_name*

        Called when the server rejects a stored replay id, so that the
        subscription can fall back to a replay option instead of retrying the
        same unusable position. Implementations which store nothing have
        nothing to do here.

        :param topic_name: Name of the subscribed topic
        """

    async def get_fetch_position(self, topic_name: str) -> FetchPosition:
        """Return the replay preset and replay id to start a subscription with

        :param topic_name: Name of the topic to subscribe to
        :return: A ``(replay_preset, replay_id)`` pair. The replay id is \
        empty unless a stored marker is being resumed from.
        """
        marker = await self.get_replay_marker(topic_name)
        if marker:
            return pb2.CUSTOM, marker
        return self.default_option.value, b""


class MappingStorage(ReplayMarkerStorage):
    """Mapping based replay marker storage"""

    def __init__(
        self,
        mapping: MutableMapping[str, bytes],
        default_option: ReplayOption = ReplayOption.NEW_EVENTS,
    ) -> None:
        """
        :param mapping: A MutableMapping object for storing replay markers
        :param default_option: The replay option to use for a subscription \
        which has no stored replay marker yet
        :raise TypeError: If *mapping* is not a MutableMapping
        """
        if not isinstance(mapping, abc.MutableMapping):
            raise TypeError(
                "mapping parameter should be an instance of MutableMapping."
            )
        super().__init__(default_option=default_option)
        #: A MutableMapping object for storing replay markers
        self.mapping = mapping

    def __repr__(self) -> str:
        """Formal string representation"""
        cls_name = type(self).__name__
        return (
            f"{cls_name}(mapping={reprlib.repr(self.mapping)}, "
            f"default_option={self.default_option!r})"
        )

    async def set_replay_marker(self, topic_name: str, replay_id: bytes) -> None:
        self.mapping[topic_name] = replay_id

    async def get_replay_marker(self, topic_name: str) -> bytes | None:
        try:
            return self.mapping[topic_name]
        except KeyError:
            return None

    async def clear_replay_marker(self, topic_name: str) -> None:
        self.mapping.pop(topic_name, None)


class ConstantReplayId(ReplayMarkerStorage):
    """A replay marker storage which starts every subscription from the same
    replay option

    .. note::

        This implementation doesn't actually store anything for later
        retrieval.
    """

    def __repr__(self) -> str:
        """Formal string representation"""
        cls_name = type(self).__name__
        return f"{cls_name}(default_option={self.default_option!r})"

    async def set_replay_marker(self, topic_name: str, replay_id: bytes) -> None:
        pass

    async def get_replay_marker(self, topic_name: str) -> bytes | None:
        return None

    async def clear_replay_marker(self, topic_name: str) -> None:
        pass


#: The types accepted by the ``replay`` parameter of
#: :obj:`~simple_salesforce_pubsub.client.SalesforcePubSubClient`
ReplayParameter = ReplayOption | ReplayMarkerStorage | MutableMapping[str, bytes]


def create_replay_storage(replay_param: ReplayParameter) -> ReplayMarkerStorage | None:
    """Create a :obj:`ReplayMarkerStorage` object from *replay_param*

    :param replay_param: One of the supported *replay_param* type objects
    :return: A new :obj:`ReplayMarkerStorage` object, or *replay_param* itself \
    if it's already a :obj:`ReplayMarkerStorage`, or ``None`` if the type of \
    *replay_param* is not supported
    """
    if isinstance(replay_param, ReplayMarkerStorage):
        return replay_param
    if isinstance(replay_param, ReplayOption):
        return ConstantReplayId(replay_param)
    if isinstance(replay_param, abc.MutableMapping):
        return MappingStorage(replay_param)
    return None
