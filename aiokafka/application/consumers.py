import asyncio
import enum
import collections
import logging

from kafka.common import TopicPartition

from .tasks import Partitionable, PartitionArgument

log = logging.getLogger(__name__)


class ConsumerState(enum.Enum):
    INIT = enum.auto()
    IDLE = enum.auto()
    READ = enum.auto()
    PROCESS = enum.auto()
    JOIN = enum.auto()
    COMMIT = enum.auto()
    CLOSED = enum.auto()


class PartitionableConsumer(Partitionable):
    """
    A stream on topics to be used by `PartitionTask`
    """

    def __init__(self, app, topics, cache=0):
        self._app = app
        self._topics = frozenset(topics)
        self._assignment = None
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

    def partitions(self):
        partitions = collections.defaultdict(set)
        for tp in self.assignment:
            partitions[tp.topic].add(tp.partition)
        if not partitions: return set()
        assert set(partitions) == self._topics
        _, out = partitions.popitem()
        for pts in partitions.values():
            assert pts == out
        return out

    def partitionate(self, partition):
        partitions = (tp for tp in self.assignment if tp.partition == partition)
        return PartitionConsumer(self._app, partitions, cache=self._cache)


class PartitionConsumer(PartitionArgument):
    """
    A stream on a group of partitions, used by `PartitionTask`
    """

    def __init__(self, app, partitions, cache=0):
        self._app = app
        # the partitions read by htis stream
        self._assignment = frozenset(partitions)
        self._cache = cache
        # all the dequeues of each each partition
        self._state = ConsumerState.INIT
        self._queues = (collections.defaultdict(collections.deque)
                if cache else None)
        self._fill_task = None # the task to fill the cache when not reading
        self._join = None # notify it to interrupt read and start to join
        self._commit = None # notify it when ready to commit
        self._commit_state = None # the state after the commit is finished
        self._ready = None # notify it when ready to read

    async def start(self):
        assert self._state is ConsumerState.INIT
        self._join = asyncio.Future()
        self._closed = asyncio.Future()

    async def close(self):
        # already closed
        if self._state is ConsumerState.CLOSED: return
        self._state = ConsumerState.CLOSED
        # clean everything that could take space
        self._assignment = None
        self.self._queues = None
        # stop the filling task if any
        if self._fill_task:
            self._fill_task.cancel()
            self._fill_task = None
        # wake up everybody to acknowledge the new state
        if self._join:
            self._join.set_result(None)
            self._join = None
        if self._commit:
            self._commit.set_result(None)
            self._commit = None
        if self._ready:
            self._ready.set_result(None)
            self._ready = None

    @property
    def topics(self):
        return set(tp.topic for top in self._assignment)

    @property
    def assignment(self):
        return self._assignment

    async def before_commit(self):
        # already committing, nothing to do
        if self._state in (ConsumerState.COMMIT, ConsumerState.CLOSED):
            return
        # The stream is not busy, can commit now
        elif self._state in (
                ConsumerState.INIT, ConsumerState.IDLE):
            self._state = ConsumerState.COMMIT
            self._commit_state = self._state
        # the stream is reading or processing, commit on read or pause
        elif self._state in (
                ConsumerState.READ, ConsumerState.PROCESS, ConsumerState.JOIN):
            # wake up the stream if it is in reading state
            self._join.set_result(None)
            self._join = asyncio.Future()
            # safest assumption
            self._commit_state = ConsumerState.PROCESS
            self._state = ConsumerState.JOIN
            if not self._commit:
                self._commit = asyncio.Future()
            await asyncio.shield(self._commit)
            assert self._state in (ConsumerState.COMMIT, ConsumerState.CLOSED)
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def after_commit(self):
        if self._state is ConsumerState.CLOSED: return
        elif self._state is ConsumerState.COMMIT:
            # restore the state
            self._state = self._commit_state
            # notify ready for reading
            if self._ready:
                self._ready.set_result(None)
                self._ready = None
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def pause(self):
        """
        Mark the processing as finished and the stream as ready for commit
        """
        # wasn't particularly busy, just change the state
        if self._state in (ConsumerState.INIT, ConsumerState.IDLE,
                ConsumerState.PROCESS):
            self._state = ConsumerState.IDLE
            # ensure that the filling task is runing
            if not self._fill_task:
                if await self._fill_cache_once():
                    self._fill_task = asyncio.ensure_future(self._fill_cache())
        # was in commit or waiting for one, notify it
        elif self._state in (ConsumerState.COMMIT, ConsumerState.JOIN):
            self._commit_state = ConsumerState.IDLE
            if self._state is ConsumerState.JOIN:
                if self._commit:
                    self._commit.set_result(None)
                    self._commit = None
            self._state = ConsumerState.COMMIT
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def wait(self, aws=None, *, timeout=None, **kwargs):
        """
        Mark the stream as paused, and wait for whatever is passed
        Useful for building a list of futures for `asyncio.wait`

        If `aws` is a collection of awaitables, equivalent to
                stream.pause()
                return await asyncio.wait(aws, timeout=timeout, **kwargs)
        If passed only a timeout, equivalent to
                stream.pause()
                await asyncio.sleep(timeout)
        else, equivalent to
                stream.pause()
                await asyncio.Future()
        """
        self.pause()
        if isinstance(aws, (int, float)) and timeout is None:
            timeout, aws = aws, None
        if aws:
            return await asyncio.wait(aws, timeout=timeout, **kwargs)
        elif timeout is None:
            await asyncio.Future()
        elif timeout > 0:
            await asyncio.sleep(timeout)

    async def getmany(self, *partitions, timeout_ms=0, max_records=None,
            max_records_per_partition=None):
        if not partitions: partitions = self._assignment
        await self._get_read(partitions)
        try:
            # check in cache first (this call is not asynchronous)
            records = await self._get_many_from_cache(partitions,
                    max_records, max_records_per_partition)
            # else request data
            if records is None or not records and timeout_ms > 0:
                records = await self._read_or_fail(
                        self._app.consumer.getmany, *partitions,
                        timeout_ms=timeout_ms, max_records=max_records,
                        max_records_per_partition=max_records_per_partition)
            # update offsets
            for tp, rec in records.items():
                self._app._offsets[tp] = rec[-1].offset + 1
        finally:
            # refill the cache (this part is not asynchronous)
            if self._state is ConsumerState.READ:
                self._state = ConsumerState.PROCESS
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
                msg = await self._read_or_fail(self._app.consumer.getone,
                        *partitions)
            # update the offset
            tp = TopicPartition(msg.topic, msg.partition)
            self._app._offsets[tp] = msg.offset + 1
            return msg
        finally:
            # refill the cache (this part is not asynchronous)
            if self._state is ConsumerState.READ:
                self._state = ConsumerState.PROCESS
                if await self._fill_cache_once():
                    self._fill_task = asyncio.ensure_future(self._fill_cache())

    async def _get_read(self, partitions):
        """
        Deal with commits and wait for the stream to be ready to read
        """
        assert self._state is not ConsumerState.CLOSED
        # if we try to read more from a partition, commit all its messages
        for tp in partitions:
            offset = self._app._offsets.get(tp)
            if offset is not None:
                self._app._commits[tp] = offset
        # if join required, join now
        if self._state in (ConsumerState.JOIN, ConsumerState.COMMIT):
            await self._do_join()
        # the only states allowed before reading
        assert self._state in (
                ConsumerState.INIT, ConsumerState.IDLE, ConsumerState.PROCESS)
        # cancel the current fetching task
        if self._fill_task:
            self._fill_task.cancel()
            self._fill_task = None
        # finally ready to read
        self._state = ConsumerState.READ

    async def _read_or_fail(self, fun, *kargs, **kwargs):
        """
        Calls the fetching function while looking for commits and close
        """
        while True:
            # maybe a CLOSED state
            assert self._state is ConsumerState.READ
            # wait for the fecth task, but also for commit and close
            task = asyncio.ensure_future(fun(*kargs, **kwargs))
            try:
                await asyncio.wait((task, self._join),
                        return_when=asyncio.FIRST_COMPLETED)
            # cancelled, just cancel the task and raise
            except:
                assert not task.done()
                task.cancel()
                raise
            # the task is done, return its result or its exception
            if task.done():
                # check that the state is still compatible with a read
                # if in join state, still return the data to avoid losing it
                # the only other expected state is CLOSED
                assert self._state in (ConsumerState.READ, ConsumerState.JOIN)
                return task.result()
            # something else happened, cancel the task and check
            else:
                task.cancel()
                # requesting for a join, do it before restarting the task
                if self._state in (ConsumerState.JOIN, ConsumerState.COMMIT):
                    await self._do_join()
                    self._state = ConsumerState.READ

    async def _do_join(self):
        """
        repeat the joining process until reading is ready
        """
        while True:
            self._commit_state = ConsumerState.PROCESS
            # notify the that joining is ready
            if self._state is ConsumerState.JOIN:
                if self._commit:
                    self._commit.set_result(None)
                    self._commit = None
            self._state = ConsumerState.COMMIT
            # wait for commit to finish
            if not self._ready:
                self._ready = asyncio.Future()
            await asyncio.shield(self._ready)
            # expected state after the commit
            if self._state is ConsumerState.PROCESS:
                break
            # a new commit has started already
            elif self._state in (ConsumerState.JOIN, ConsumerState.COMMIT):
                continue
            # maybe CLOSED state
            else: raise RuntimeError('state ??? ' + self._state.name)

    async def _get_many_from_cache(self, partitions, max_records,
            max_records_per_partition):
        """
        A call to get data from the cache, eventually completed by the consumer
        Not asynchronous
        Returns `None` if `consumer.getmany` should be called manually
        """
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
        # timeout is 0, so not asynchronous call
        for tp, records in await self._app.consumer.getmany(
                *req_pts, max_records=req_mr,
                max_records_per_partition=req_mrpp, timeout_ms=0):
            previous = cached.setdefault(tp, records)
            if previous is not records: previous.extend(records)
        return cached

    async def _fill_cache_once(self, timeout_ms=0):
        """
        Makes one call to try to fill the cache
        Asynchronous only when `timeout_ms` is not `0`
        """
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
        """
        A task to fill the cache
        """
        while await self._fill_cache_once(timeout_ms): pass
