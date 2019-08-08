import asyncio
import enum
import logging
import collections

from kafka.common import TopicPartition

from .tasks import Partitionable, PartitionArgument

log = logging.getLogger(__name__)


class TableState(enum.Enum):
    INIT = enum.auto()
    IDLE = enum.auto()
    SEND = enum.auto()
    JOIN = enum.auto()
    COMMIT = enum.auto()
    CLOSED = enum.auto()


class PartitionableMemoryTable(Partitionable):

    def __init__(self, app, topic):
        self._app = app
        self._topic = topic

    @property
    def topics(self):
        return (self._topic,)

    def partitionate(self, partition):
        return PartitionMemoryTable(self._app,
                TopicPartition(self._topic, partition))


class PartitionMemoryTable(PartitionArgument, collections.MutableMapping):

    def __init__(self, app, partition):
        self._app = app
        self._partition = partition
        self._state = TableState.INIT
        self._data = {}
        self._queue = collections.deque()
        self._send_task = None
        self._send_future = None
        self._flushed_future = None

    @property
    def topics(self):
        return (self._partition.topic,)

    @property
    def assignment(self):
        return (self._partition,)

    @property
    def partition(self):
        return self._partition.partition

    async def restore(self):
        async with self._app.restoration_stream(self._partition) as stream:
            async for msg in stream:
                if msg.value is not None:
                    self._data[msg.key] = msg.value
                else:
                    self._data.discard(msg.key)

    async def start(self):
        await self.restore()
        self._send_task = asyncio.ensure_future(self._send_all())

    async def stop(self):
        if self._state is TableState.CLOSED:
            return
        self._state = TableState.CLOSED
        if self._send_future:
            self._send_future.cancel()
            self._send_future = None
        if self._flushed_future:
            self._flushed_future.cancel()
            self._flushed_future = None

    async def _send_all(self):
        while True:
            if self._state is TableState.CLOSED:
                return
            elif self._queue:
                self._state = TableState.SEND
                while self._queue:
                    key, value = self._queue.popleft()
                    await self._app.send(key=key, value=value,
                            topic=self._partition.topic,
                            partition=self._partition.partition)
                if self._state is TableState.JOIN:
                    self._state = TableState.COMMIT
                    self._flushed_future.set_result(None)
                    self._flushed_future = None
            else:
                self._state = TableState.IDLE
                if self._flushed_future:
                    self._flushed_future.set_result(None)
                    self._flushed_future = None
                if not self._send_future:
                    self._send_future = asyncio.Future()
                await self._send_future

    def _send(self, key, value=None):
        assert self._state in (
                TableState.INIT, TableState.IDLE, TableState.SEND)
        self._queue.append((key, value))
        if self._send_future:
            self._send_future.set_result(None)
            self._send_future = None

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if value is not None:
            self._data[key] = value
        else:
            self._data.discard(key)
        self._send(key, value)

    def __delitem__(self, key):
        self._data.discard(key)
        self._send(key)

    def __contains__(self, key):
        return key in self._data

    def __len__(self, key):
        return len(self.data)

    def __iter__(self):
        return self.data.keys()

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    async def during_commit(self):
        # already committing, nothing to do
        if self._state in (TableState.COMMIT, TableState.CLOSED):
            return
        # The stream is not busy, can commit now
        elif self._state in (TableState.INIT, TableState.IDLE):
            self._state = TableState.COMMIT
        elif self._state in (TableState.SEND, TableState.JOIN):
            self._state = TableState.JOIN
            if not self._flushed_future:
                self._flushed_future = asyncio.Future()
            await asyncio.shield(self._flushed_future)
            assert self._state is TableState.COMMIT
        else:
            raise RuntimeError('state ??? ' + self._state.name)

    async def after_commit(self):
        self._state = TableState.IDLE
