import asyncio
import logging
import collections
from enum import Enum

from aiokafka.client import AIOKafkaClient
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.producer import AIOKafkaProducer
from .protocol import CreateTopicsRequest
from kafka.common import TopicPartition
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment)

from aiokafka.errors import (
    NotControllerError,
    TopicAlreadyExistsError,
    for_code,
)

from aiokafka import __version__

log = logging.getLogger(__name__)

_missing = object()


class Topic:

    CONFIG_NAMES = {
        'retention_ms': 'retention.ms',
        'retention_bytes': 'retention.bytes',
    }

    def __init__(self, app, topic,
            partitions=None,
            replicas=None,
            compacting=None,
            deleting=None,
            retention_ms=None,
            retention_bytes=None,
            config=None):
        self._app = app
        self._topic = topic
        self._replicas = replicas
        self._compacting = compacting
        self._deleting = deleting
        self._retention_ms = retention_ms
        self._retention_bytes = retention_bytes
        self._config = config

    def request(self, partitions=None):
        partitions = partitions or self.partitions
        config = {}
        if self._compacting is not None or self._deleting is not None:
            flags = []
            if self._compacting: flags.append('compact')
            if self._deleting: flags.append('delete')
            config['log.cleanup.policy'] = ','.join(sorted(flags))
        if self._retention_ms is not None:
            config['retention.ms'] = self._retention_ms
        if self._retention_bytes is not None:
            config['retention.bytes'] = self._retention_bytes
        if self._config: config.update(self._config)
        return (self._topic, partitions, self._replicas,
                [], list(config.items()))


class StreamState(Enum):
    INIT = 1
    READY = 2
    WAITING = 3
    PROCESSING = 4
    STOPPED = 5


class TopicStream:

    def __init__(self, app, loop, topics):
        self._app = app
        self._loop = loop
        self._state = INIT
        self.topics = frozenset(topics)
        self._assignement = None
        self._reassignement_start = loop.create_future()
        self._reassignement_stop = loop.create_future()
        if self._app.rebalancing:
            self._before_reassignement()
        else:
            self._after_reassignement()
        self._watcher = loop.create_task(self._watch_reassignement())

    @property
    def assignment(self):
        return self._assignment

    async def _watch_reassignement(self):
        while True:
            done = await self._app.watch_reassignement()
            if done: self._after_reassignement()
            else: self._before_reassignement()

    def _before_reassignement(self):
        if self._reassignement_stop.done():
            self._reassignement_stop = self._loop.create_future()
        self._reassignement_start.set_result(None)

    def _after_reassignement(self):
        self._assignement = set()
        for tp in self._app.consumer.assignment:
            if tp.topic in self.topics:
                self._assignement.add(tp)
        if self._reassignement_start.done():
            self._reassignement_start = self._loop.create_future()
        self._reassignement_stop.set_result(None)

    async def _get(self, fun, **kwargs):
        while True:
            await shield(self._reassignement_stop)
            task = self._loop.create_task(fun(*self._assignment, **kwargs))
            try:
                done, _ = await asyncio.wait((coro, self._reassignement_start),
                        loop=self._loop, return_when=FIRST_COMPLETED)
            except asyncio.CancelledError:
                assert not task.done()
                task.cancel()
                raise
            if task.done():
                msg = await task.result()
                self._state = PROCESSING
                return msg
            else:
                task.cancel()

    async def getone(self):
        return await self._get(self._app.consumer.getone)

    async def getmany(self, timeout_ms=0, max_records=None,
            max_records_per_partition=None):
        return await self._get(self._app.consumer.getmany,
                timeout_ms=timeout_ms, max_records=max_records,
                max_records_per_partition=max_records_per_partition)

    async def __anext__(self):
        return await self._get(self._app.consumer.getone)

    def __aiter__(self):
        return self


class PartitionStream:

    def __init__(self, app, loop, assignment):
        self._app = app
        self._loop = loop
        self._state = INIT
        self.assignment = frozenset(assignment)
        self._reassignement = self._loop.create_future()
        if self._app.rebalancing:
            raise Exception('rebalanced')

    def _watch_reassignement(self):
        while True:
            done = await self._app.watch_reassignement()
            if done: continue
            self.assignment = None
            self._reassignement.set_result(None)
            return

    async def _get(self, fun, **kwargs):
        self._state = READY
        if self.assignment is None:
            raise Exception('rebalanced')
        task = self._loop.create_task(fun(*self._assignment, **kwargs))
        try:
            done, _ = await asyncio.wait((coro, self._reassignement),
                    loop=self._loop, return_when=FIRST_COMPLETED)
        except asyncio.CancelledError:
            assert not task.done()
            task.cancel()
            raise
        if task.done():
            msg = await task.result()
            self._state = PROCESSING
            return msg
        else:
            task.cancel()
            raise Exception('rebalanced')

    async def getone(self):
        return await self._get(self._app.consumer.getone)

    async def getmany(self, timeout_ms=0, max_records=None,
            max_records_per_partition=None):
        return await self._get(self._app.consumer.getmany,
                timeout_ms=timeout_ms, max_records=max_records,
                max_records_per_partition=max_records_per_partition)

    async def __anext__(self):
        return await self._get(self._app.consumer.getone)

    def __aiter__(self):
        return self


class _AIOKafkaConsumer(AIOKafkaConsumer):

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
                 partition_assignment_strategy=(RoundRobinPartitionAssignor,),
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

        if topics:
            topics = self._validate_topics(topics)
            self._client.set_topics(topics)
            self._subscription.subscribe(topics=topics)

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


class Assignator:

    name = 'application'
    version = 0

    def __init__(self, app):
        self._app = app

    def assign(self, cluster, member_metadata):

        all_topics = None
        for metadata in member_metadata.values():
            if all_topics is None:
                all_topics = set(metadata.subscription)
            elif all_topics != set(metadata.subscription):
                diff = all_topics.symmetric_difference(metadata.subscription)
                raise UnmergeableTopcis(
                        'Topic(s) %s do not appear in all members',
                        ', '.join(diff))

        group_topics = collections.defaultdict(list)
        for topic in all_topics:
            group_topics[self._app._topic_group.get(topic, topic)]\
                    .append(topic)

        assignment = collections.defaultdict(lambda: collections.defaultdict(list))
        member_iter = itertools.cycle(sorted(member_metadata.keys()))

        for group in group_topics.values():
            all_partitions = None
            for topic in all_topics:
                partitions = cluster.partitions_for_topic(topic)
                if partitions is None:
                    raise UnmergeableTopcis(
                            'No partition metadata for topic %s', topic)
                if all_partitions is None:
                    all_partitions = set(partitions)
                elif all_partitions != set(partitions):
                    diff = all_partitions.symmetric_difference(partitions)
                    raise UnmergeableTopcis(
                            'Partition(s) %s do not appear in all topics',
                            ', '.join(str(p) for p in diff))
            all_partitions = sorted(all_partitions)

            for partition in all_partitions:
                member_id = next(member_iter)
                for topic in all_topics:
                    assignment[member_id][topic].append(partition)

        protocol_assignment = {}
        for member_id in member_metadata:
            protocol_assignment[member_id] = ConsumerProtocolMemberAssignment(
                cls.version,
                sorted(assignment[member_id].items()),
                b'')
        return protocol_assignment

    @classmethod
    def metadata(cls, topics):
        return ConsumerProtocolMemberMetadata(cls.version, list(topics), b'')

    @classmethod
    def on_assignment(cls, assignment):
        pass


class AIOKafkaApplication(object):
    _APPLICATION_CLIENT_ID_SEQUENCE

    def __init__(self, loop,
            bootstrap_servers='localhost',
            client_id='aiokafka-' + __version__,
            group_id=None,
            key_deserializer=None, value_deserializer=None,
            fetch_max_wait_ms=500,
            fetch_max_bytes=52428800,
            fetch_min_bytes=1,
            max_partition_fetch_bytes=1 * 1024 * 1024,
            metadata_max_age_ms=300000,
            acks=-1,
            key_serializer=None, value_serializer=None,
            compression_type=None,
            max_batch_size=16384,
            partitioner=DefaultPartitioner(),
            max_request_size=1048576,
            linger_ms=0,
            send_backoff_ms=100,
            enable_idempotence=False,
            transactional_id=None,
            request_timeout_ms=40 * 1000,
            retry_backoff_ms=100,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            check_crcs=True,
            metadata_max_age_ms=5 * 60 * 1000,
            partition_assignment_strategy=None,
            max_poll_interval_ms=300000,
            rebalance_timeout_ms=None,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            consumer_timeout_ms=200,
            max_poll_records=None,
            max_poll_records_per_partition=None,
            ssl_context=None,
            security_protocol='PLAINTEXT',
            api_version='auto',
            exclude_internal_topics=True,
            connections_max_idle_ms=540000,
            isolation_level="read_uncommitted",
            sasl_mechanism="PLAIN",
            sasl_plain_password=None,
            sasl_plain_username=None,
            sasl_kerberos_service_name='kafka',
            sasl_kerberos_domain_name=None,
            create_topic_timeout_ms=600000):

        if partition_assignment_strategy is None:
            partition_assignment_strategy = (Assignator(self),)

        if acks not in (0, 1, -1, 'all', _missing):
            raise ValueError("Invalid ACKS parameter")
        if compression_type not in ('gzip', 'snappy', 'lz4', None):
            raise ValueError("Invalid compression type!")
        if compression_type:
            checker, compression_attrs = \
                    AIOKafkaProducer._COMPRESSORS[compression_type]
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
            if acks is _missing:
                acks = -1
            elif acks not in ('all', -1):
                raise ValueError(
                    "acks={} not supported if enable_idempotence=True"
                    .format(acks))
            self._txn_manager = TransactionManager(
                transactional_id, transaction_timeout_ms, loop=loop)
        else:
            self._txn_manager = None

        if acks is _missing:
            acks = 1
        elif acks == 'all':
            acks = -1

        AIOKafkaProducer._APPLICATION_CLIENT_ID_SEQUENCE += 1
        if client_id is None:
            client_id = 'aiokafka-application-%s' % \
                AIOKafkaProducer._APPLICATION_CLIENT_ID_SEQUENCE

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._compression_type = compression_type
        self._partitioner = partitioner
        self._max_request_size = max_request_size
        self._request_timeout_ms = request_timeout_ms

        self.client = AIOKafkaClient(
            loop=loop, bootstrap_servers=bootstrap_servers,
            client_id=client_id, metadata_max_age_ms=metadata_max_age_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            api_version=api_version,
            ssl_context=ssl_context,
            security_protocol=security_protocol,
            connections_max_idle_ms=connections_max_idle_ms,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_kerberos_service_name=sasl_kerberos_service_name,
            sasl_kerberos_domain_name=sasl_kerberos_domain_name)
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

        self.consumer = AIOKafkaConsumer(
            loop=loop, client=self.client,
            group_id=group_id,
            acks=acks,
            compression_type=compression_type,
            fetch_max_wait_ms=fetch_max_wait_ms,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            max_partition_fetch_bytes=max_partition_fetch_bytes,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            check_crcs=check_crcs,
            partition_assignment_strategy=partition_assignment_strategy,
            max_poll_interval_ms=max_poll_interval_ms,
            rebalance_timeout_ms=rebalance_timeout_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            consumer_timeout_ms=consumer_timeout_ms,
            max_poll_records=max_poll_records,
            max_poll_records_per_partition=max_poll_records_per_partition,
            exclude_internal_topics=exclude_internal_topics,
            isolation_level=isolation_level,
        )

        self._create_topic_timeout_ms = create_topic_timeout_ms

        self._topics = {}
        self._group_topics = [[]]
        self._topic_group = {True: 0}

        self._loop = loop
        if loop.get_debug():
            self._source_traceback = traceback.extract_stack(sys._getframe(1))
        self._closed = False

    def topic(self, topic, group=None,
              partitions=None,
              replicas=None,
              compacting=None,
              deleting=None,
              retention_ms=None,
              retention_bytes=None,
              config=None,
              required=False):
        if group == topic: group = None
        if topic in self._topics:
            raise ValueError(f'topic {topic} already declared')
        out = Topic(self, topic,
            partitions=partitions,
            replicas=replicas,
            compacting=compacting,
            deleting=deleting,
            retention_ms=retention_ms,
            retention_bytes=retention_bytes,
            config=config,
            required=required)
        if group:
            if group is not True and group not in self._topics:
                raise ValueError(f'unknown group {group}')
            elif group in self._topic_group:
                index = self._topic_group[group]
                self._topic_group[topic] = index
                self._group_topics[index].append(topic)
            else:
                index = len(self._group_topics)
                self._topic_group[group] = index
                self._topic_group[topic] = index
                self._group_topics.append([group, topic])
        self._topics[topic] = out
        return out

    def check_topics(self, connection):
        # TODO use `node_id = self.client.get_random_node()`
        group_partitions = [None for _ in self._group_topics]
        tocreate = set()
        for topic in self._topics:
            partitions = self.client.cluster.partitions_for_topic(topic)
            if partitions is None:
                tocreate.add(topic)
            elif topic in self._topic_group:
                group = self._topic_group[topic]
                if group_partitions[group] is None:
                    group_partitions[group] = partitions
                elif group_partitions[group] != partitions:
                    raise RuntimeError('different partitions')

        if not tocreate: return
        topics = []
        for topic in tocreate:
            partitions = None
            if topic in self._topic_group:
                partitions = group_partitions[self._topic_group[topic]]
                if partitions is not None:
                    assert partitions = set(range(len(partitions)))
                    partitions = len(partitions)
            topics.append(self._topics[topic].request(partitions))

        if self.client.api_version < (0, 10):
            request = CreateTopicsRequest[0](topics,
                    self._create_topic_timeout_ms)
        else:
            request = CreateTopicsRequest[1](topics,
                    self._create_topic_timeout_ms, False)
        response = await connection.send(request,
                timeout=self._create_topic_timeout_ms / 1000)

        for topic, code, reason in response.topic_errors:
            if code != 0:
                if code == TopicAlreadyExistsError.errno:
                    pass
                # elif code == NotControllerError.errno:
                #     raise RuntimeError(f'Invalid controller: {controller_node}')
                else:
                    raise for_code(code)(
                        f'Cannot create topic: {topic} ({code}): {reason}')

        if self.client.api_version < (0, 10):
            metadata_request = MetadataRequest[0]([])
        else:
            metadata_request = MetadataRequest[1]([])
        metadata = await connection.send(request,
                timeout=self._create_topic_timeout_ms / 1000)
        self.client.cluster.update_metadata(metadata)

        for topic in self._topics:
            partitions = self.client.cluster.partitions_for_topic(topic)
            if partitions is None:
                raise RuntimeError(f'No topic {topic}')
            elif topic in self._topic_group:
                group = self._topic_group[topic]
                if group_partitions[group] is None:
                    group_partitions[group] = partitions
                elif group_partitions[group] != partitions:
                    raise RuntimeError('different partitions')

    # Warn if producer was not closed properly
    # We don't attempt to close the Consumer, as __del__ is synchronous
    def __del__(self, _warnings=warnings):
        if self._closed is False:
            if PY_36:
                kwargs = {'source': self}
            else:
                kwargs = {}
            _warnings.warn("Unclosed AIOKafkaProducer {!r}".format(self),
                           ResourceWarning,
                           **kwargs)
            context = {'producer': self,
                       'message': 'Unclosed AIOKafkaProducer'}
            if self._source_traceback is not None:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)

    async def start(self):
        """Connect to Kafka cluster and check server version"""
        log.debug("Starting the Kafka producer")  # trace
        await self.client.bootstrap()

        if self._compression_type == 'lz4':
            assert self.client.api_version >= (0, 8, 2), \
                'LZ4 Requires >= Kafka 0.8.2 Brokers'

        if self._txn_manager is not None and self.client.api_version < (0, 11):
            raise UnsupportedVersionError(
                "Idempotent producer available only for Broker vesion 0.11"
                " and above")

        await self._sender.start()
        self._message_accumulator.set_api_version(self.client.api_version)
        self._producer_magic = 0 if self.client.api_version < (0, 10) else 1
        log.debug("Kafka producer started")

        await self.consumer.start()

    async def flush(self):
        """Wait untill all batches are Delivered and futures resolved"""
        await self._message_accumulator.flush()

    async def stop(self):
        """Flush all pending data and close all connections to kafka cluster"""
        if self._closed:
            return
        self._closed = True

        await self.consumer.close()

        # If the sender task is down there is no way for accumulator to flush
        if self._sender is not None and self._sender.sender_task is not None:
            await asyncio.wait([
                self._message_accumulator.close(),
                self._sender.sender_task],
                return_when=asyncio.FIRST_COMPLETED,
                loop=self._loop)

            await self._sender.close()

        await self.client.close()
        log.debug("The Kafka producer has closed.")

    async def partitions_for(self, topic):
        """Returns set of all known partitions for the topic."""
        return (await self.client._wait_on_metadata(topic))

    def _serialize(self, topic, key, value):
        if self._key_serializer:
            serialized_key = self._key_serializer(key)
        else:
            serialized_key = key
        if self._value_serializer:
            serialized_value = self._value_serializer(value)
        else:
            serialized_value = value

        message_size = LegacyRecordBatchBuilder.record_overhead(
            self._producer_magic)
        if serialized_key is not None:
            message_size += len(serialized_key)
        if serialized_value is not None:
            message_size += len(serialized_value)
        if message_size > self._max_request_size:
            raise MessageSizeTooLargeError(
                "The message is %d bytes when serialized which is larger than"
                " the maximum request size you have configured with the"
                " max_request_size configuration" % message_size)

        return serialized_key, serialized_value

    def _partition(self, topic, partition, key, value,
                   serialized_key, serialized_value):
        if partition is not None:
            assert partition >= 0
            assert partition in self._metadata.partitions_for_topic(topic), \
                'Unrecognized partition'
            return partition

        all_partitions = list(self._metadata.partitions_for_topic(topic))
        available = list(self._metadata.available_partitions_for_topic(topic))
        return self._partitioner(
            serialized_key, all_partitions, available)

    async def send(
        self, topic, value=None, key=None, partition=None,
        timestamp_ms=None, headers=None
    ):
        """Publish a message to a topic.

        Arguments:
            topic (str): topic where the message will be published
            value (optional): message value. Must be type bytes, or be
                serializable to bytes via configured value_serializer. If value
                is None, key is required and message acts as a 'delete'.
                See kafka compaction documentation for more details:
                http://kafka.apache.org/documentation.html#compaction
                (compaction requires kafka >= 0.8.1)
            partition (int, optional): optionally specify a partition. If not
                set, the partition will be selected using the configured
                'partitioner'.
            key (optional): a key to associate with the message. Can be used to
                determine which partition to send the message to. If partition
                is None (and producer's partitioner config is left as default),
                then messages with the same key will be delivered to the same
                partition (but if key is None, partition is chosen randomly).
                Must be type bytes, or be serializable to bytes via configured
                key_serializer.
            timestamp_ms (int, optional): epoch milliseconds (from Jan 1 1970
                UTC) to use as the message timestamp. Defaults to current time.

        Returns:
            asyncio.Future: object that will be set when message is
            processed

        Raises:
            kafka.KafkaTimeoutError: if we can't schedule this record (
                pending buffer is full) in up to `request_timeout_ms`
                milliseconds.

        Note:
            The returned future will wait based on `request_timeout_ms`
            setting. Cancelling the returned future **will not** stop event
            from being sent, but cancelling the ``send`` coroutine itself
            **will**.
        """
        assert value is not None or self.client.api_version >= (0, 8, 1), (
            'Null messages require kafka >= 0.8.1')
        assert not (value is None and key is None), \
            'Need at least one: key or value'

        # first make sure the metadata for the topic is available
        await self.client._wait_on_metadata(topic)

        # Ensure transaction is started and not committing
        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if txn_manager.transactional_id is not None and \
                    not self._txn_manager.is_in_transaction():
                raise IllegalOperation(
                    "Can't send messages while not in transaction")

        if headers is not None:
            if self.client.api_version < (0, 11):
                raise UnsupportedVersionError(
                    "Headers not supported before Kafka 0.11")
        else:
            # Record parser/builder support only list type, no explicit None
            headers = []

        key_bytes, value_bytes = self._serialize(topic, key, value)
        partition = self._partition(topic, partition, key, value,
                                    key_bytes, value_bytes)

        tp = TopicPartition(topic, partition)
        log.debug("Sending (key=%s value=%s) to %s", key, value, tp)

        fut = await self._message_accumulator.add_message(
            tp, key_bytes, value_bytes, self._request_timeout_ms / 1000,
            timestamp_ms=timestamp_ms, headers=headers)
        return fut

    async def send_and_wait(
        self, topic, value=None, key=None, partition=None,
        timestamp_ms=None
    ):
        """Publish a message to a topic and wait the result"""
        future = await self.send(
            topic, value, key, partition, timestamp_ms)
        return (await future)

    def create_batch(self):
        """Create and return an empty BatchBuilder.

        The batch is not queued for send until submission to ``send_batch``.

        Returns:
            BatchBuilder: empty batch to be filled and submitted by the caller.
        """
        return self._message_accumulator.create_builder()

    async def send_batch(self, batch, topic, *, partition):
        """Submit a BatchBuilder for publication.

        Arguments:
            batch (BatchBuilder): batch object to be published.
            topic (str): topic where the batch will be published.
            partition (int): partition where this batch will be published.

        Returns:
            asyncio.Future: object that will be set when the batch is
                delivered.
        """
        # first make sure the metadata for the topic is available
        await self.client._wait_on_metadata(topic)
        # We only validate we have the partition in the metadata here
        partition = self._partition(topic, partition, None, None, None, None)

        # Ensure transaction is started and not committing
        if self._txn_manager is not None:
            txn_manager = self._txn_manager
            if txn_manager.transactional_id is not None and \
                    not self._txn_manager.is_in_transaction():
                raise IllegalOperation(
                    "Can't send messages while not in transaction")

        tp = TopicPartition(topic, partition)
        log.debug("Sending batch to %s", tp)
        future = await self._message_accumulator.add_batch(
            batch, tp, self._request_timeout_ms / 1000)
        return future

    def _ensure_transactional(self):
        if self._txn_manager is None or \
                self._txn_manager.transactional_id is None:
            raise IllegalOperation(
                "You need to configure transaction_id to use transactions")

    async def begin_transaction(self):
        self._ensure_transactional()
        log.debug(
            "Beginning a new transaction for id %s",
            self._txn_manager.transactional_id)
        await asyncio.shield(
            self._txn_manager.wait_for_pid(),
            loop=self._loop
        )
        self._txn_manager.begin_transaction()

    async def commit_transaction(self):
        self._ensure_transactional()
        log.debug(
            "Committing transaction for id %s",
            self._txn_manager.transactional_id)
        self._txn_manager.committing_transaction()
        await asyncio.shield(
            self._txn_manager.wait_for_transaction_end(),
            loop=self._loop
        )

    async def abort_transaction(self):
        self._ensure_transactional()
        log.debug(
            "Aborting transaction for id %s",
            self._txn_manager.transactional_id)
        self._txn_manager.aborting_transaction()
        await asyncio.shield(
            self._txn_manager.wait_for_transaction_end(),
            loop=self._loop
        )

    def transaction(self):
        return TransactionContext(self)

    async def send_offsets_to_transaction(self, offsets, group_id):
        self._ensure_transactional()

        if not self._txn_manager.is_in_transaction():
            raise IllegalOperation("Not in the middle of a transaction")

        if not group_id or not isinstance(group_id, str):
            raise ValueError(group_id)

        # validate `offsets` structure
        formatted_offsets = commit_structure_validate(offsets)

        log.debug(
            "Begin adding offsets %s for consumer group %s to transaction",
            formatted_offsets, group_id)
        fut = self._txn_manager.add_offsets_to_txn(formatted_offsets, group_id)
        await asyncio.shield(fut, loop=self._loop)
