import pytest

from simple_salesforce_pubsub import pubsub_api_pb2 as pb2
from simple_salesforce_pubsub.replay import (
    ConstantReplayId,
    MappingStorage,
    ReplayMarkerStorage,
    ReplayOption,
    create_replay_storage,
)


class ReplayMarkerStorageStub(ReplayMarkerStorage):
    def __init__(self, marker=None, **kwargs):
        super().__init__(**kwargs)
        self.marker = marker
        self.stored = []

    async def get_replay_marker(self, topic_name):
        return self.marker

    async def set_replay_marker(self, topic_name, replay_id):
        self.stored.append((topic_name, replay_id))


def test_replay_option_values_are_replay_presets():
    assert ReplayOption.NEW_EVENTS == pb2.LATEST
    assert ReplayOption.ALL_EVENTS == pb2.EARLIEST


@pytest.mark.asyncio
async def test_get_fetch_position_without_marker():
    storage = ReplayMarkerStorageStub()

    assert await storage.get_fetch_position("/event/X__e") == (pb2.LATEST, b"")


@pytest.mark.asyncio
async def test_get_fetch_position_honours_default_option():
    storage = ReplayMarkerStorageStub(default_option=ReplayOption.ALL_EVENTS)

    assert await storage.get_fetch_position("/event/X__e") == (pb2.EARLIEST, b"")


@pytest.mark.asyncio
async def test_get_fetch_position_with_marker_uses_custom_preset():
    storage = ReplayMarkerStorageStub(marker=b"\x01\x02")

    assert await storage.get_fetch_position("/event/X__e") == (pb2.CUSTOM, b"\x01\x02")


@pytest.mark.asyncio
async def test_mapping_storage_round_trip():
    mapping = {}
    storage = MappingStorage(mapping)

    assert await storage.get_replay_marker("/event/X__e") is None

    await storage.set_replay_marker("/event/X__e", b"marker")

    assert mapping == {"/event/X__e": b"marker"}
    assert await storage.get_replay_marker("/event/X__e") == b"marker"


def test_mapping_storage_rejects_non_mapping():
    with pytest.raises(TypeError):
        MappingStorage([])


@pytest.mark.asyncio
async def test_constant_replay_id_never_stores():
    storage = ConstantReplayId(ReplayOption.ALL_EVENTS)

    await storage.set_replay_marker("/event/X__e", b"marker")

    assert await storage.get_replay_marker("/event/X__e") is None
    assert await storage.get_fetch_position("/event/X__e") == (pb2.EARLIEST, b"")


def test_create_replay_storage_variants():
    storage = ReplayMarkerStorageStub()
    mapping = {}

    assert create_replay_storage(storage) is storage
    assert isinstance(create_replay_storage(ReplayOption.ALL_EVENTS), ConstantReplayId)

    from_mapping = create_replay_storage(mapping)
    assert isinstance(from_mapping, MappingStorage)
    assert from_mapping.mapping is mapping

    assert create_replay_storage(object()) is None
