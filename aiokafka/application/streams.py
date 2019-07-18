import asyncio
import enum
import collections

from kafka.common import TopicPartition

from .tasks import Partitionable, PartitionArgument


class PartitionableStream(Partitionable):

    def __init__(self, app, topics, cache=0):
        self._app = app
        self._topics = frozenset(topics)
        self._cache = cache

    @property
    def topics(self):
        return self._topics

    @property
    def assignment(self):
        if self._assignment is None:
            self._assignment = frozenset(tp
                    for tp in self._app.consumer.assignment()
                    if tp.topic in self.topics)
        return self._assignment

    async def before_rebalance(self, revoked):
        self._assignment = None

    async def partitions(self):
        partitions = collections.defaultdict(set)
        for tp in self.assignment:
            partitions[tp.topic].add(tp.partition)
        if not partitions: return set()
        assert set(partitions) == self._topics
        _, out = partitions.popitem()
        for pts in partitions.values():
            assert tps == out
        return out

    def partitionate(self, partition):
        partitions = (tp for tp in self.assignment if tp.partition == partition)
        return PartitionStream(self._app, partitions, cache=self._cache)


class PartitionStream(PartitionArgument):

    def __init__(self, app, partitions, cache=0)
        self._app = app
        self._assignment = frozenset(partitions)
        self._cache = cache
        self._queues = collections.defaultdict(collections.deque)
        self._fill_task = None
        self._join = None
        self._commit = None
        self._closed = None

    async def start(self):
        self._join = asyncio.Future()
        self._closed = asyncio.Future()

    @property
    def topics(self):
        return set(tp.topic for top in self._assignment)

    @property
    def assignment(self):
        return self._assignment

    async def before_commit(self):
        # assert self._state is not StreamState.CLOSED
        if self._state in (StreamState.INIT, StreamState.PAUSE):
            state = self._state
            if await self._do_join():
                assert self._state is StreamState.COMMIT
                self._state = state
        elif self._state in (StreamState.READ, StreamState.PROCESS):
            self._state = StreamState.JOIN
            if StreamState.READ:
                self._join.set_result(None)
                self._join = asyncio.Future()
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def getmany(self, *partitions, timeout_ms=0, max_records=None,
            max_records_per_partition=None):
        if not partitions: partitions = self._assignment
        await self._get_read(partitions)
        try:
            # check in cache first
            records = await self._get_many_from_cache(partitions,
                    max_records, max_records_per_partition)
            # else request data
            if records is None or not records and timeout_ms > 0:
                records = await self._get_or_fail(
                        self._app.consumer.getmany, *partitions,
                        timeout_ms=timeout_ms, max_records=max_records,
                        max_records_per_partition=max_records_per_partition)
            for tp, rec in records.items():
                self._app._offsets[tp] = rec[-1].offset + 1
        finally:
            # refill the cache
            if self._state is StreamState.READ:
                self._state = StreamState.PROCESS
                if await self._fill_cache_once():
                    self._fill_task = asyncio.ensure_future(self._fill_cache())

    async def getone(self, *partitions):
        if not partitions: partitions = self._assignment
        await self._get_read(partitions)
        try:
            # look for a message in cache first
            msg = None
            if self._queues:
                for tp in partitions:
                    queue = self._queues.get(tp)
                    if queue:
                        msg = queue.popleft()
                        break
            # else request data
            if not msg:
                msg = await self._get_or_fail(self._app.consumer.getone,
                        *partitions)
            tp = TopicPartition(msg.topic, msg.partition)
            self._app._offsets[tp] = msg.offset + 1
            return msg
        finally:
            # refill the cache
            if self._state is StreamState.READ:
                self._state = StreamState.PROCESS
                if await self._fill_cache_once():
                    self._fill_task = asyncio.ensure_future(self._fill_cache())

    async def _get_read(self, partitions):
        # check that the state allows reading
        assert self._state in (StreamState.PROCESS, StreamState.JOIN,
            StreamState.INIT, StreamState.IDLE, StreamState.PAUSE)
        # if we try to read more from a partition, commit all its messages
        for tp in partitions:
            offset = self._app._offsets.get(tp)
            if offset is not None:
                self._app._commits[tp] = offset
        # if join required, join now
        while self._state is StreamState.JOIN:
            if not await asyncio.shield(self._do_join()):
                raise RuntimeError('rebalanced')
            # should always be the case, but maybe could have 2 join in a row
            if self._state is StreamState.COMMIT: break
            elif self._state is StreamState.JOIN: continue
            else: raise RuntimeError('state ??? ' + self._state.name)
        # cancel the current fetching task
        if self._fill_task:
            self._fill_task.cancel()
            self._fill_task = None
        self._state = StreamState.READ

    async def _get_or_fail(self, fun, *kargs, **kwargs):
        while True:
            assert self._state is StreamState.READ
            task = asyncio.ensure_future(fun(*kargs, **kwargs))
            try:
                await asyncio.wait((task, self._closed, self._join),
                        return_when=asyncio.FIRST_COMPLETED)
            except asyncio.CancelledError:
                assert not task.done()
                task.cancel()
                raise
            if task.done():
                return task.result()
            else:
                task.cancel()
                if self._state is StreamState.JOIN:
                    self._commit = asyncio.Future()
                    self._join.set_result(None)
                    self._join = None
                    await asyncio.shield(self._commit)
                    if self._state is StreamState.COMMIT:
                        self._state = StreamState.READ
                if self._state is StreamState.CLOSED:
                    raise RuntimeError('rebalanced')

    async def _get_many_from_cache(self, partitions, max_records,
            max_records_per_partition):
        if not self._queues: return None
        # build the function get_limit for faster read of mrpp
        if max_records_per_partition is None:
            get_limit = lambda tp: float('inf')
        elif isinstance(max_records_per_partition, dict):
            def get_limit(tp):
                limit = max_records_per_partition.get(tp)
                return limit if limit is not None else float('inf')
        else: get_limit = lambda tp: max_records_per_partition
        if max_records is None: max_records = float('inf')
        # all the matching records in cache
        cached = {}
        # the parameters for the next request
        req_pts = set()
        req_mrpp = {}
        # check in cache
        for tp in partitions:
            limit = get_limit(tp)
            if limit < 1: continue
            queue = self._queues.get(tp, ())
            count = min(limit, max_records, len(queue))
            if count < 1:
                req_pts.add(pt)
                if limit != float('inf'):
                    req_mrpp[tp] = limit - count
                continue
            cached[tp] = [queue.popleft() for _ in range(count)]
            max_records_per_partition -= count
            if max_records_per_partition < 1: return cached
            if count < limit:
                req_pts.add(pt)
                if limit != float('inf'):
                    req_mrpp[tp] = limit - count
        # nothing in cache, return for a normal call
        if not cached: return None
        # or nothing to fetch more, return
        if not req_pts: return cached
        # call `getmany` without timeout to fill up the response
        req_mr = max_records if max_records != float('inf') else None
        for tp, records in await self._app.consumer.getmany(
                *req_pts, max_records=req_mr,
                max_records_per_partition=req_mrpp, timeout_ms=0):
            previous = cached.setdefault(tp, records)
            if previous is not records: previous.extend(records)
        return cached

    async def _fill_cache_once(self, timeout_ms=0):
        if not self._cache: return False
        # build request limits
        req_pts = set()
        req_mrpp = {}
        for tp in self._assignment:
            count = self._cache - len(self._queues.get(tp, ()))
            if count < 1: continue
            req_pts.add(tp)
            req_mrpp[tp] = count
        # cache is full, wait for next `get_many` call
        if not req_pts: return False
        results = await self._app.consumer.getmany(*req_pts,
                timeout_ms=timeout_ms, max_records_per_partition=req_mrpp)
        for tp, records in results.items():
            self._queues[tp].extend(records)
        return True

    async def _fill_cache(self, timeout_ms=1000):
        while await self._fill_cache_once(timeout_ms): pass
