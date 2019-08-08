
import asyncio
import logging
import collections

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.producer import AIOKafkaProducer
from kafka.common import TopicPartition
from aiokafka.errors import UnsupportedVersionError
from aiokafka.consumer.subscription_state import SubscriptionState
from aiokafka.consumer.fetcher import Fetcher
from aiokafka.consumer.group_coordinator import (
        GroupCoordinator, NoGroupCoordinator)
from kafka.partitioner.default import DefaultPartitioner
from aiokafka.util import INTEGER_MAX_VALUE
from aiokafka.producer.transaction_manager import TransactionManager
from aiokafka.producer.message_accumulator import MessageAccumulator
from aiokafka.producer.sender import Sender

log = logging.getLogger(__name__)


class ClientConsumer(AIOKafkaConsumer):

    def __init__(self, loop, client,
                 group_id=None,
                 fetch_max_wait_ms=500,
                 fetch_max_bytes=52428800,
                 fetch_min_bytes=1,
                 max_partition_fetch_bytes=1 * 1024 * 1024,
                 request_timeout_ms=40 * 1000,
                 retry_backoff_ms=100,
                 auto_offset_reset='latest',
                 enable_auto_commit=True,
                 auto_commit_interval_ms=5000,
                 check_crcs=True,
                 partition_assignment_strategy=(),
                 consumer_protocol=None,
                 max_poll_interval_ms=300000,
                 rebalance_timeout_ms=None,
                 session_timeout_ms=10000,
                 heartbeat_interval_ms=3000,
                 consumer_timeout_ms=200,
                 max_poll_records=None,
                 max_poll_records_per_partition=None,
                 exclude_internal_topics=True,
                 isolation_level="read_uncommitted"):
        if max_poll_records is not None and (
                not isinstance(max_poll_records, int) or max_poll_records < 1):
            raise ValueError("`max_poll_records` should be positive Integer")
        if max_poll_records_per_partition is not None and (
                not isinstance(max_poll_records_per_partition, int) or
                        max_poll_records_per_partition < 1):
            raise ValueError("`max_poll_records_per_partition` should be "
                "positive Integer")

        if rebalance_timeout_ms is None:
            rebalance_timeout_ms = session_timeout_ms

        self._client = client

        self._group_id = group_id
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._session_timeout_ms = session_timeout_ms
        self._retry_backoff_ms = retry_backoff_ms
        self._auto_offset_reset = auto_offset_reset
        self._request_timeout_ms = request_timeout_ms
        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._partition_assignment_strategy = partition_assignment_strategy
        self._consumer_protocol = consumer_protocol
        self._key_deserializer = None
        self._value_deserializer = None
        self._fetch_min_bytes = fetch_min_bytes
        self._fetch_max_bytes = fetch_max_bytes
        self._fetch_max_wait_ms = fetch_max_wait_ms
        self._max_partition_fetch_bytes = max_partition_fetch_bytes
        self._exclude_internal_topics = exclude_internal_topics
        self._max_poll_records = max_poll_records
        self._max_poll_records_per_partition = max_poll_records_per_partition
        self._consumer_timeout = consumer_timeout_ms / 1000
        self._isolation_level = isolation_level
        self._rebalance_timeout_ms = rebalance_timeout_ms
        self._max_poll_interval_ms = max_poll_interval_ms

        self._check_crcs = check_crcs
        self._subscription = SubscriptionState(loop=loop)
        self._fetcher = None
        self._coordinator = None
        self._loop = loop

        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._closed = False

    async def start(self):
        """ Connect to Kafka cluster. This will:

            * Load metadata for all cluster nodes and partition allocation
            * Wait for possible topic autocreation
            * Join group if ``group_id`` provided
        """
        assert self._fetcher is None, "Did you call `start` twice?"
        await self._wait_topics()

        if self._client.api_version < (0, 9):
            raise ValueError("Unsupported Kafka version: {}".format(
                self._client.api_version))

        if self._isolation_level == "read_committed" and \
                self._client.api_version < (0, 11):
            raise UnsupportedVersionError(
                "`read_committed` isolation_level available only for Brokers "
                "0.11 and above")

        self._fetcher = Fetcher(
            self._client, self._subscription, loop=self._loop,
            key_deserializer=self._key_deserializer,
            value_deserializer=self._value_deserializer,
            fetch_min_bytes=self._fetch_min_bytes,
            fetch_max_bytes=self._fetch_max_bytes,
            fetch_max_wait_ms=self._fetch_max_wait_ms,
            max_partition_fetch_bytes=self._max_partition_fetch_bytes,
            check_crcs=self._check_crcs,
            fetcher_timeout=self._consumer_timeout,
            retry_backoff_ms=self._retry_backoff_ms,
            auto_offset_reset=self._auto_offset_reset,
            isolation_level=self._isolation_level)

        if self._group_id is not None:
            # using group coordinator for automatic partitions assignment
            self._coordinator = GroupCoordinator(
                self._client, self._subscription, loop=self._loop,
                group_id=self._group_id,
                heartbeat_interval_ms=self._heartbeat_interval_ms,
                session_timeout_ms=self._session_timeout_ms,
                retry_backoff_ms=self._retry_backoff_ms,
                enable_auto_commit=self._enable_auto_commit,
                auto_commit_interval_ms=self._auto_commit_interval_ms,
                assignors=self._partition_assignment_strategy,
                consumer_protocol=self._consumer_protocol,
                exclude_internal_topics=self._exclude_internal_topics,
                rebalance_timeout_ms=self._rebalance_timeout_ms,
                max_poll_interval_ms=self._max_poll_interval_ms
            )
            if self._subscription.subscription is not None:
                if self._subscription.partitions_auto_assigned():
                    # Either we passed `topics` to constructor or `subscribe`
                    # was called before `start`
                    await self._subscription.wait_for_assignment()
                else:
                    # `assign` was called before `start`. We did not start
                    # this task on that call, as coordinator was yet to be
                    # created
                    self._coordinator.start_commit_offsets_refresh_task(
                        self._subscription.subscription.assignment)
        else:
            # Using a simple assignment coordinator for reassignment on
            # metadata changes
            self._coordinator = NoGroupCoordinator(
                self._client, self._subscription, loop=self._loop,
                exclude_internal_topics=self._exclude_internal_topics)

            if self._subscription.subscription is not None:
                if self._subscription.partitions_auto_assigned():
                    # Either we passed `topics` to constructor or `subscribe`
                    # was called before `start`
                    await self._client.force_metadata_update()
                    self._coordinator.assign_all_partitions(check_unknown=True)

    async def stop(self):
        """ Close the consumer, while waiting for finilizers:

            * Commit last consumed message if autocommit enabled
            * Leave group if used Consumer Groups
        """
        if self._closed:
            return
        log.debug("Closing the KafkaConsumer.")
        self._closed = True
        if self._coordinator:
            await self._coordinator.close()
        if self._fetcher:
            await self._fetcher.close()
        # await self._client.close()
        log.debug("The KafkaConsumer has closed.")

    async def consumed(self, partition):
        highwater = self.highwater(partition)
        position = await self.position(partition)
        return highwater is not None and position is not None and \
                highwater <= position

    def create_poll_waiter(self):
        return self._fetcher.create_poll_waiter()

    def consumed_offsets(self):
        subscription = self._subscription.subscription
        if subscription is None:
            return None
        assignment = subscription.assignment
        if assignment is None:
            return None
        return assignment.all_consumed_offsets()


class ClientProducer(AIOKafkaProducer):

    def __init__(self, loop, client,
                 request_timeout_ms=40000,
                 acks=None,
                 key_serializer=None,
                 value_serializer=None,
                 compression_type=None,
                 max_batch_size=16384,
                 partitioner=DefaultPartitioner(),
                 max_request_size=1048576,
                 linger_ms=0,
                 send_backoff_ms=100,
                 retry_backoff_ms=100,
                 enable_idempotence=False,
                 transactional_id=None,
                 transaction_timeout_ms=60000):
        if acks not in (0, 1, -1, 'all', None):
            raise ValueError("Invalid ACKS parameter")
        if compression_type not in ('gzip', 'snappy', 'lz4', None):
            raise ValueError("Invalid compression type!")
        if compression_type:
            checker, compression_attrs = self._COMPRESSORS[compression_type]
            if not checker():
                raise RuntimeError("Compression library for {} not found"
                                   .format(compression_type))
        else:
            compression_attrs = 0

        if transactional_id is not None:
            enable_idempotence = True
        else:
            transaction_timeout_ms = INTEGER_MAX_VALUE

        if enable_idempotence:
            if acks is None:
                acks = -1
            elif acks not in ('all', -1):
                raise ValueError(
                    "acks={} not supported if enable_idempotence=True"
                    .format(acks))
            self._txn_manager = TransactionManager(
                transactional_id, transaction_timeout_ms, loop=loop)
        else:
            self._txn_manager = None

        if acks is None:
            acks = 1
        elif acks == 'all':
            acks = -1

        AIOKafkaProducer._PRODUCER_CLIENT_ID_SEQUENCE += 1

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._compression_type = compression_type
        self._partitioner = partitioner
        self._max_request_size = max_request_size
        self._request_timeout_ms = request_timeout_ms

        self.client = client
        self._metadata = self.client.cluster
        self._message_accumulator = MessageAccumulator(
            self._metadata, max_batch_size, compression_attrs,
            self._request_timeout_ms / 1000, txn_manager=self._txn_manager,
            loop=loop)
        self._sender = Sender(
            self.client, acks=acks, txn_manager=self._txn_manager,
            retry_backoff_ms=retry_backoff_ms, linger_ms=linger_ms,
            message_accumulator=self._message_accumulator,
            request_timeout_ms=request_timeout_ms,
            loop=loop)

        self._loop = loop
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._closed = False


class RestorationConsumer(AIOKafkaConsumer):

    def __init__(self, *kargs, **kwargs):
        super().__init__(*kargs, **kwargs)
        self._partitions = set()
        self._start_future = asyncio.Future()
        self._restoration_streams = set()

    async def start(self):
        partitions = self._partitions
        self._partitions = None
        self.assign(partitions)
        await super().start()
        self._start_future.set_result(None)

    async def stop(self):
        await super().stop()
        for stream in self._restoration_streams:
            await stream.stop()

    def restoration_stream(self, *partitions):
        if self._partitions.intersection(partitions):
            raise RuntimeError('already restoring %s' % ', '.join(partitions))
        self._partitions.update(partitions)
        stream = RestorationStream(self, partitions)
        self._restoration_streams.add(stream)
        return stream

    async def stream_finished(self, stream):
        self._restoration_streams.remove(stream)
        if not self._restoration_streams:
            await self.stop()

    async def consumed(self, partition):
        highwater = self.highwater(partition)
        position = await self.position(partition)
        return highwater is not None and position is not None and \
                highwater <= position

    def create_poll_waiter(self):
        return self._fetcher.create_poll_waiter()

    def consumed_offsets(self):
        subscription = self._subscription.subscription
        if subscription is None:
            return None
        assignment = subscription.assignment
        if assignment is None:
            return None
        return assignment.all_consumed_offsets()


class RestorationStream:

    def __init__(self, consumer, partitions):
        self._consumer = consumer
        self._partitions = partitions
        self._started = False
        self._queue = collections.deque()
        self._fill_task = None

    async def __aenter__(self):
        if self._queue is None:
            raise RuntimeError('closed')
        assert not self._started
        self._started = True
        await self._consumer._start_future
        return self

    async def __aexit__(self, type, value, traceback):
        await self.stop()

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.next()

    async def next(self):
        if self._queue is None:
            raise RuntimeError('closed')
        while not self._queue:
            if not self._fill_task:
                self._fill_task = asyncio.ensure_future(self._fill())
            await asyncio.shield(self._fill_task)
        return self._queue.popleft()

    async def _fill(self):
        while True:
            print (await asyncio.gather(*(self._consumer.position(partition)
                    for partition in self._partitions)),
                    [self._consumer.highwater(partition) for partition in self._partitions])
            results = await self._consumer.getmany(
                    *self._partitions, timeout_ms=0)
            if results:
                self._queue.extend(
                        msg for msgs in results.values() for msg in msgs)
                self._fill_task = None
                return
            elif all(await asyncio.gather(*(self._consumer.consumed(partition)
                    for partition in self._partitions))):
                raise StopAsyncIteration('consumed')
            else:
                await self._consumer.create_poll_waiter()

    async def stop(self):
        if self._queue is None:
            return
        self._queue = None
        self._started = False
        if self._fill_task:
            self._fill_task.cancel()
        await self._consumer.stream_finished(self)
