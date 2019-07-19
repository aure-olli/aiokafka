import asyncio
import enum

from kafka.common import TopicPartition

from .tasks import Partitionable, PartitionArgument


class ProducerState(enum.Enum):
    INIT = enum.auto()
    IDLE = enum.auto()
    SEND = enum.auto()
    JOIN = enum.auto()
    COMMIT = enum.auto()
    CLOSED = enum.auto()


class PartitionableProducer(Partitionable):

    def __init__(self, app, topics):
        self._app = app
        self._topics = frozenset(topics)

    @property
    def topics(self):
        return self._topics

    def partitionate(self, partition):
        return PartitionProducer(self._app, self._topics, partition)


class PartitionProducer(PartitionArgument):

    def __init__(self, app, topics, partition):
        self._topics = frozenset(topics)
        self._partition = partition
        self._state = ProducerState.INIT
        self._sending = set() # a set of all the runing sending futures
        self._ready = None # notify it when ready to send

    @property
    def topics(self):
        return self._topics

    @property
    def assignment(self):
        return set(TopicPartition(topic, self._partition)
                for topic in self._topics)

    async def before_commit(self):
        # already committing, nothing to do
        if self._state in (ProducerState.COMMIT, ProducerState.CLOSED):
            return
        # The stream is not busy, can commit now
        elif self._state in (ConsumerState.INIT, ConsumerState.IDLE):
            self._state = ConsumerState.COMMIT
        # The stream is sending, wait for all the `send` calls to finish
        elif self._state in (ConsumerState.SEND, ConsumerState.JOIN):
            if self._sending:
                self._state = ConsumerState.JOIN
                await asyncio.wait(self._sending)
                assert not self._sending and self._state is ConsumerState.JOIN
            self._state = ConsumerState.COMMIT
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def after_commit(self):
        if self._state is ConsumerState.CLOSED: return
        elif self._state is ConsumerState.COMMIT:
            self._state = ConsumerState.IDLE
            # notify ready for sending
            if self._ready:
                self._ready.set_result(None)
                self._ready = None
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def send(self, topic=None, value=None, key=None, partition=None,
            timestamp_ms=None, headers=None):
        topic, partition = await self._get_send(topic, partition)
        try:
            future = asyncio.Future()
            self._sending.add(future)
            return await self._app.send(topic=topic, value=value, key=key,
                    partition=partition, timestamp_ms=timestamp_ms,
                    headers=headers)
        finally:
            self._sending.discard(future)
            future.set_result(None)
            if not self._sending and self._state is ProducerState.SEND:
                self._state = ProducerState.IDLE

    async def send_and_wait(self, topic=None, value=None, key=None,
            partition=None, timestamp_ms=None, headers=None):
        future = await self.send(topic, value=value, key=key,
                partition=partition, timestamp_ms=timestamp_ms,
                headers=headers)
        return await future

    async def send_batch(self, batch, topic=None, *, partition=None):
        topic, partition = await self._get_send(topic, partition)
        try:
            future = asyncio.Future()
            self._sending.add(future)
            return await self._app.send_batch(batch, topic=topic,
                    partition=partition)
        finally:
            self._sending.discard(future)
            future.set_result(None)
            if not self._sending and self._state is ProducerState.SEND:
                self._state = ProducerState.IDLE

    async def _get_send(self, topic, partition):
        # check topic and partition
        if topic is None:
            topic, = self._topics
        else:
            assert topic in self._topics
        assert partition is None or partition == self._partition
        # wait for commit to finish
        while self._state in (ProducerState.JOIN, ProducerState.COMMIT):
            if not self._ready:
                self._ready = asyncio.Future()
            await asyncio.shield(self._ready)
        # only can send from those states
        assert self._state in (
                ProducerState.INIT, ProducerState.IDLE, ProducerState.SEND)
        self._state = ProducerState.SEND
        return topic, self._partition
