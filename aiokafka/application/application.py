import asyncio
import logging
import collections
import itertools
import enum
import warnings

from aiokafka.client import AIOKafkaClient
from .protocol import (
    CreateTopicsRequest, ApplicationConsumerProtocolMetadata, ApplicationConsumerProtocolAssignment, ApplicationConsumerProtocol)
from kafka.protocol.metadata import MetadataRequest
from aiokafka.abc import ConsumerRebalanceListener
from kafka.partitioner.default import DefaultPartitioner
from aiokafka.producer.message_accumulator import MessageAccumulator
from aiokafka.producer.sender import Sender
from aiokafka.producer.transaction_manager import TransactionManager

from .tasks import PartitionTask
from .consumers import PartitionableConsumer
from .producers import PartitionableProducer
from .utils import ClientConsumer, RestorationConsumer,ClientProducer
from .tables import PartitionableMemoryTable

from aiokafka.errors import (
    NotControllerError,
    TopicAlreadyExistsError,
    for_code,
)

from aiokafka import __version__

log = logging.getLogger(__name__)

_missing = object()


class TopicConfig:

    CONFIG_NAMES = {
        'retention_ms': 'retention.ms',
        'retention_bytes': 'retention.bytes',
    }

    def __init__(self, app, topic,
            partitions,
            replicas,
            compacting=None,
            deleting=None,
            retention_ms=None,
            retention_bytes=None,
            config=None):
        self._app = app
        self._topic = topic
        self._partitions = partitions
        self._replicas = replicas
        self._compacting = compacting
        self._deleting = deleting
        self._retention_ms = retention_ms
        self._retention_bytes = retention_bytes
        self._config = config

    def request(self, partitions=None):
        partitions = partitions or self._partitions or 10
        replicas = self._replicas or 1
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
        return (self._topic, partitions, replicas,
                [], list(config.items()))


class Assignator:

    name = 'application'
    version = 0

    def __init__(self, app):
        self._app = app

    def groups(self, topics):
        groups = collections.defaultdict(list)
        for topic in topics:
            groups[self._app._topic_group.get(topic, topic)].append(topic)
        for group in groups.values():
            group.sort()
        return sorted(groups.values())

    def assign(self, cluster, member_metadata):
        groups = None
        for metadata in member_metadata.values():
            if groups is None:
                groups = metadata.groups
            else:
                assert groups == metadata.groups

        assignment = collections.defaultdict(lambda: collections.defaultdict(list))
        member_iter = itertools.cycle(sorted(member_metadata.keys()))

        for group in groups:
            all_partitions = None
            for topic in group:
                partitions = cluster.partitions_for_topic(topic)
                if partitions is None:
                    raise RuntimeError(
                            'No partition metadata for topic %s', topic)
                if all_partitions is None:
                    all_partitions = set(partitions)
                elif all_partitions != set(partitions):
                    diff = all_partitions.symmetric_difference(partitions)
                    raise RuntimeError(
                            'Partition(s) %s do not appear in all topics',
                            ', '.join(str(p) for p in diff))
            all_partitions = sorted(all_partitions)

            for partition in all_partitions:
                member_id = next(member_iter)
                for topic in group:
                    assignment[member_id][topic].append(partition)

        protocol_assignment = {}
        for member_id in member_metadata:
            protocol_assignment[member_id] = \
                ApplicationConsumerProtocolAssignment(
                    self.version,
                    sorted(assignment[member_id].items()))
        return protocol_assignment

    def metadata(self, topics):
        return ApplicationConsumerProtocolMetadata(
                self.version, self.groups(topics))

    def on_assignment(self, assignment):
        pass


class RebalanceListener(ConsumerRebalanceListener):
    """
    A `ConsumerRebalanceListener` for committing offsets
    and rebuilding processors
    """
    def __init__(self, app):
        self._app = app

    async def on_partitions_revoked(self, revoked):
        print ('on_partitions_revoked begin')
        await self._app.before_rebalance(revoked)
        print ('on_partitions_revoked end')

    async def on_partitions_assigned(self, assigned):
        print ('on_partitions_assigned')
        await self._app.after_rebalance(assigned)
        print ('on_partitions_assigned end')


def catch_on_error(fun):
    async def aux(self, *kargs, **kwargs):
        try:
            return await fun(self, *kargs, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await self.on_error(e)
            raise
    aux.__name__ = fun.__name__
    return aux


class ApplicationState(enum.Enum):
    INIT = enum.auto()
    PROCESS = enum.auto()
    COMMIT = enum.auto()
    REBALANCE = enum.auto()
    ABORT = enum.auto()


class Application:

    def __init__(self, loop=None,
            bootstrap_servers='localhost',
            client_id='aiokafka-' + __version__,
            group_id=None,
            key_deserializer=None, value_deserializer=None,
            fetch_max_wait_ms=500,
            fetch_max_bytes=52428800,
            fetch_min_bytes=1,
            max_partition_fetch_bytes=1 * 1024 * 1024,
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
            transaction_timeout_ms=60000,
            request_timeout_ms=40 * 1000,
            retry_backoff_ms=100,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            check_crcs=True,
            metadata_max_age_ms=5 * 60 * 1000,
            partition_assignment_strategy=None,
            consumer_protocol=ApplicationConsumerProtocol,
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
            create_topic_timeout_ms=60000,
            create_topic_refresh_ms=100,

            topic_partitions=None,
            topic_replicas=None,
            topic_compacting=None,
            topic_deleting=None,
            topic_retention_ms=None,
            topic_retention_bytes=None,
            topic_config=None):

        self._client_options = dict(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            metadata_max_age_ms=metadata_max_age_ms,
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
            sasl_kerberos_domain_name=sasl_kerberos_domain_name,
        )

        self._consumer_options = dict(
            fetch_max_wait_ms=fetch_max_wait_ms,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            max_partition_fetch_bytes=max_partition_fetch_bytes,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            enable_auto_commit=False,
            check_crcs=check_crcs,
            consumer_protocol=consumer_protocol,
            max_poll_interval_ms=max_poll_interval_ms,
            rebalance_timeout_ms=rebalance_timeout_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            consumer_timeout_ms=consumer_timeout_ms,
            max_poll_records=max_poll_records,
            max_poll_records_per_partition=max_poll_records_per_partition,
        )
        self._main_consumer_options = dict(
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            partition_assignment_strategy=partition_assignment_strategy,
            isolation_level=isolation_level,
            **self._consumer_options,
        )
        self._table_consumer_options = dict(
            auto_offset_reset='earliest',
            partition_assignment_strategy=None,
            isolation_level='read_committed',
            **self._consumer_options,
        )
        self._producer_options = dict(
            request_timeout_ms=request_timeout_ms,
            compression_type=compression_type,
            max_batch_size=max_batch_size,
            partitioner=partitioner,
            max_request_size=max_request_size,
            linger_ms=linger_ms,
            send_backoff_ms=send_backoff_ms,
            retry_backoff_ms=retry_backoff_ms,
            enable_idempotence=enable_idempotence,
            transactional_id=transactional_id,
            transaction_timeout_ms=transaction_timeout_ms,
        )

        self._topic_options = dict(
            partitions=topic_partitions,
            replicas=topic_replicas,
            compacting=topic_compacting,
            deleting=topic_deleting,
            retention_ms=topic_retention_ms,
            retention_bytes=topic_retention_bytes,
            config=topic_config,
        )

        self.client = None
        self.consumer = None
        self.producer = None
        self._has_txn = transactional_id is not None or enable_idempotence

        self._enable_auto_commit = enable_auto_commit
        self._auto_commit_interval_ms = auto_commit_interval_ms
        self._auto_commit_task = None

        self._create_topic_timeout_ms = create_topic_timeout_ms
        self._create_topic_refresh = create_topic_refresh_ms / 1000
        self._subscriptions = set()
        self._equipartitioned = list()
        self._topic_configs = {}
        self._topic_group = None

        self._offsets = {}
        self._commits = {}
        self._tasks = set()
        self._restoration_consumer = None
        self._restoration_streams = []

        self._state = ApplicationState.INIT
        self._ready_waiter = None
        self._assign_waiter = None
        self._join_task = None
        self._commit_task = None
        self._rebalance_state = None

        self._loop = None

    @property
    def ready(self):
        return self._state is ApplicationState.INIT or not self._ready_waiter

    @property
    def assigned(self):
        return self._state is ApplicationState.INIT or not self._assign_waiter

    @catch_on_error
    async def start(self):
        assert self._state is ApplicationState.INIT
        self._loop = asyncio.get_event_loop()
        self.client = AIOKafkaClient(loop=self._loop, **self._client_options)
        await self.client.bootstrap()
        await self.check_topics()
        self.producer = ClientProducer(
                loop=self._loop, client=self.client, **self._producer_options)
        await self.producer.start()
        if self._has_txn:
            await self.producer.begin_transaction()
        self.consumer = ClientConsumer(
                loop=self._loop, client=self.client, **self._consumer_options)
        self.consumer.subscribe(list(self._subscriptions),
                listener=RebalanceListener(self))
        await self.consumer.start()
        self._state = ApplicationState.PROCESS
        if not self._auto_commit_task:
            self._auto_commit_task = asyncio.ensure_future(
                    self._do_auto_commit())

    def ensure_topic(self, topic,
              partitions,
              replicas,
              group=None,
              *,
              compacting=None,
              deleting=None,
              retention_ms=None,
              retention_bytes=None,
              config=None):
        if group is not None and group != topic:
            self._equipartitioned.append({topic, group})
        if group == topic: group = None
        if topic in self._topic_configs:
            raise ValueError(f'topic {topic} already declared')
        self._topic_configs[topic] = TopicConfig(self, topic,
            partitions=partitions,
            replicas=replicas,
            compacting=compacting,
            deleting=deleting,
            retention_ms=retention_ms,
            retention_bytes=retention_bytes,
            config=config)

    async def check_topics(self):
        equipartitioned = {}
        for group in self._equipartitioned:
            seen = set()
            groups = [group]
            for topic in group:
                while topic not in seen:
                    seen.add(topic)
                    v = equipartitioned.get(topic)
                    if isinstance(v, set):
                        groups.append(v)
                        break
                    elif v is None:
                        break
                    else:
                        topic = v
            i, group = min(enumerate(groups), key=lambda t: len(t[1]))
            for j, g in enumerate(groups):
                if j == i: continue
                group.update(g)
            for topic in group:
                for t in seen: equipartitioned[t] = topic
            equipartitioned[topic] = group
        equipartitioned = [group for group in
                equipartitioned.values() if isinstance(group, set)]
        topic_group = {}
        for i, group in enumerate(equipartitioned):
            topic_group.update((topic, i) for topic in group)
        group_partitions = [None for _ in equipartitioned]
        self._topic_group = topic_group
        self._equipartitioned = equipartitioned

        all_topics = set(self._topic_configs)
        all_topics.update(self._subscriptions)
        all_topics.update(topic_group)

        tocreate = set()
        for topic in all_topics:
            partitions = self.client.cluster.partitions_for_topic(topic)
            if partitions is None:
                tocreate.add(topic)
            elif topic in topic_group:
                index = topic_group[topic]
                pts = group_partitions[index]
                if pts is None:
                    group_partitions[index] = partitions
                elif pts != partitions:
                    raise RuntimeError('group %s is not equipartitioned' % \
                            (tuple(equipartitioned[index]),))
        if (not self._topic_options['replicas']
                and not tocreate <= set(self._topic_configs)
            or not self._topic_options['partitions']
                and not set(topic_group[topic] for topic in tocreate) <=
                    set(index for index, pts in enumerate(group_partitions)
                        if pts is not None)):
            raise RuntimeError('topics %s do not exist' %
                    (tuple(tocreate - set(self._topic_configs)),))
        if not tocreate: return
        node_id = self.client.get_random_node()

        topics = []
        for topic in tocreate:
            partitions = None
            if topic in topic_group:
                partitions = group_partitions[topic_group[topic]]
                if partitions is not None:
                    assert partitions == set(range(len(partitions)))
                    partitions = len(partitions)
            if topic in self._topic_configs:
                config = self._topic_configs[topic]
            else:
                config = TopicConfig(topic, **self._topic_options)
            topics.append(config.request(partitions))

        print ('create', topics)
        if self.client.api_version < (0, 10):
            request = CreateTopicsRequest[0](topics,
                    self._create_topic_timeout_ms)
        else:
            request = CreateTopicsRequest[1](topics,
                    self._create_topic_timeout_ms, False)

        timeout = self._create_topic_timeout_ms / 1000
        start_time = self._loop.time()
        response = await self.client.send(node_id, request, timeout=timeout)
        timeout -= self._loop.time() - start_time

        for topic, code, reason in response.topic_errors:
            if code != 0:
                if code == TopicAlreadyExistsError.errno:
                    pass
                # elif code == NotControllerError.errno:
                #     raise RuntimeError(f'Invalid controller: {controller_node}')
                else:
                    raise for_code(code)(
                        f'Cannot create topic: {topic} ({code}): {reason}')

        while timeout > 0:
            start_time = self._loop.time()

            if self.client.api_version < (0, 10):
                metadata_request = MetadataRequest[0]([])
            else:
                metadata_request = MetadataRequest[1]([])
            metadata = await self.client.send(
                    node_id, metadata_request, timeout=timeout)
            self.client.cluster.update_metadata(metadata)

            for topic in self._topics:
                partitions = self.client.cluster.partitions_for_topic(topic)
                if partitions is None:
                    break
                elif topic in topic_group:
                    group = topic_group[topic]
                    if group_partitions[group] is None:
                        group_partitions[group] = partitions
                    elif group_partitions[group] != partitions:
                        raise RuntimeError('different partitions')
            else:
                break

            ellapsed = self._loop.time() - start_time
            timeout -= ellapsed
            sleep = min(timeout, self._create_topic_refresh - ellapsed)
            if sleep <= 0:
                await asyncio.sleep(sleep)
                timeout -= sleep
        else:
            raise RuntimeError('cannot create topic')

    def register_task(self, stream):
        self._tasks.add(stream)

    def unregister_task(self, stream):
        self._tasks.remove(stream)

    def group(self, *topics):
        if len(topics):
            self._equipartitioned.append(set(topics))

    def subscribe(self, *topics):
        self._subscriptions.update(topics)

    def partition_task(self, fun, *kargs, **kwargs):
        return PartitionTask(self, fun, kargs, kwargs)

    def partitionable_consumer(self, *topics, cache=0):
        self.group(*topics)
        self.subscribe(*topics)
        return PartitionableConsumer(self, topics, cache=cache)

    def partitionable_producer(self, *topics):
        self.group(*topics)
        return PartitionableProducer(self, topics)

    def partitionable_table(self, topic):
        self.group(topic)
        return PartitionableMemoryTable(self, topic)

    async def _start_restoration_consumer(self):
        await asyncio.sleep(0.01)
        consumer = self._restoration_consumer[0]
        self._restoration_consumer = None
        await consumer.start()

    async def _stop_restoration_streams(self):
        streams = self._restoration_streams
        self._restoration_streams = []
        if streams:
            await asyncio.wait([stream.stop() for stream in streams])

    def restoration_stream(self, *partitions):
        if not self._restoration_consumer:
            options = dict(
                loop=self._loop,
                auto_offset_reset='earliest',
                isolation_level='read_committed',
            )
            options.update(self._table_consumer_options)
            options.update(self._client_options)
            consumer = RestorationConsumer(**options)
            task = self._loop.create_task(self._start_restoration_consumer())
            self._restoration_consumer = (consumer, task)
        else:
            consumer = self._restoration_consumer[0]
        stream = consumer.restoration_stream(*partitions)
        self._restoration_streams.append(stream)
        return stream

    async def on_error(self, exc):
        log.error('on_error', exc_info=True)
        await self.abort()

    async def commit(self):
        if self._state is ApplicationState.COMMIT:
            await asyncio.shield(self._commit_task)
        elif self._state is ApplicationState.PROCESS:
            self._state = ApplicationState.COMMIT
            assert not self._join_task
            assert not self._commit_task
            if not self._ready_waiter:
                self._ready_waiter = asyncio.Future()
            self._join_task = asyncio.ensure_future(self._do_join())
            self._commit_task = asyncio.ensure_future(self._do_commit())
            await asyncio.shield(self._commit_task)
        else:
            raise RuntimeError('state ??? ' + self._state)

    @catch_on_error
    async def before_rebalance(self, revoked):
        while True:
            if self._state in (
                    ApplicationState.PROCESS, ApplicationState.INIT):
                assert not self._join_task
                assert not self._commit_task
                self._join_task = asyncio.ensure_future(self._do_join())
                break
            elif self._state is ApplicationState.COMMIT:
                assert self._commit_task
                if self._join_task:
                    self._commit_task.cancel()
                    self._commit_task = None
                    break
                try: await asyncio.shield(self._commit_task)
                except: pass
            else:
                raise RuntimeError('state ??? ' + self._state)
        if not self._assign_waiter:
            self._assign_waiter = asyncio.Future()
        if not self._ready_waiter:
            self._ready_waiter = asyncio.Future()
        self._state = ApplicationState.REBALANCE
        if self._auto_commit_task:
            self._auto_commit_task.cancel()
            self._auto_commit_task = None
        self._commit_task = asyncio.ensure_future(
                self._do_before_rebalance(revoked))
        await asyncio.shield(self._commit_task)
        self._commit_task = None

    async def _do_before_rebalance(self, revoked):
        assert self._state is ApplicationState.REBALANCE and self._assign_waiter
        tasks = [self._join_task]
        tasks.extend(task.before_rebalance(revoked) for task in self._tasks)
        await asyncio.gather(**tasks)
        await self._stop_restoration_streams()
        self._offsets.clear()
        self._commits.clear()
        self._join_task = None
        self._commit_task = None

    @catch_on_error
    async def after_rebalance(self, assigned):
        assert self._state is ApplicationState.REBALANCE and self._assign_waiter
        assert not self._join_task
        assert not self._commit_task
        self._commit_task = asyncio.ensure_future(
                self._do_after_rebalance(assigned))
        await asyncio.shield(self._commit_task)

    async def _do_after_rebalance(self, assigned):
        assert self._state is ApplicationState.REBALANCE and self._assign_waiter
        assert not self._join_task
        assert not self._commit_task
        if self._has_txn:
            await self.producer.begin_transaction()
        if self._tasks:
            tasks = [task.after_rebalance(assigned) for task in self._tasks]
            await asyncio.gather(*tasks)
        if not self._auto_commit_task:
            self._auto_commit_task = asyncio.ensure_future(
                    self._do_auto_commit())
        self._state = ApplicationState.PROCESS
        self._commit_task = None
        self._assign_waiter.set_result(None)
        self._assign_waiter = None
        self._ready_waiter.set_result(None)
        self._ready_waiter = None

    async def abort(self):
        if self._state is ApplicationState.INIT:
            return
        elif self._state is ApplicationState.ABORT:
            await asyncio.shield(self._commit_task)
        else:
            self._state = ApplicationState.ABORT
            if self._auto_commit_task:
                self._auto_commit_task.cancel()
                self._auto_commit_task = None
            if self._commit_task:
                self._commit_task.cancel()
                self._commit_task = None
            if self._join_task:
                self._join_task.cancel()
                self._join_task = None
            if self._assign_waiter:
                self._assign_waiter.cancel()
                self._assign_waiter = asyncio.Future()
            if self._ready_waiter:
                self._ready_waiter.cancel()
                self._ready_waiter = asyncio.Future()
            self._commit_task = asyncio.ensure_future(self._do_abort())
            await asyncio.shield(self._commit_task)

    async def _do_abort(self):
        tasks = [task.abort() for task in self._tasks]
        await asyncio.gather(*tasks)
        await self._stop_restoration_streams()
        self._offsets.clear()
        self._commits.clear()
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None
        if self.producer:
            await self.producer.stop()
            self.producer = None
        if self.client:
            await self.client.stop()
            self.client = None
        self._state = ApplicationState.INIT
        self._commit_task = None

    async def _do_join(self):
        """
        Wait for all tasks to be ready to commit
        Pushes all the remaining messages and offsets
        Commits the transaction
        """
        if self._tasks:
            await asyncio.gather(*(task.before_commit()
                    for task in self._tasks))
        # even offsets of unread partitions should be committed
        # to avoid using `auto_offset_reset` after a reassignment
        commits = self._group_id and self.consumer.consumed_offsets()
        if commits:
            commits.update(self._commits)
        if commits:
        # if self._group_id and self._commits:
            if self._txn_manager:
                await self.producer.send_offsets_to_transaction(
                       commits, self._group_id)
            else:
                await self.consumer.commit(commits)
        await self.producer.flush()
        if self._has_txn:
            await self.producer.commit_transaction()

    @catch_on_error
    async def _do_commit(self):
        assert self._state is ApplicationState.COMMIT
        assert self._join_task
        if self._join_task is True:
            self._join_task = asyncio.ensure_future(self._do_join())
        await asyncio.shield(self._join_task)
        self._join_task = None
        assert self._state is ApplicationState.COMMIT
        if self._txn_manager:
            await self.producer.begin_transaction()
        self.ready = True
        if self._tasks:
            await asyncio.wait([
                    task.after_commit() for task in self._tasks])
        self._state = ApplicationState.PROCESS
        self._commit_task = None
        self._ready_waiter.set_result(None)
        self._ready_waiter = None

    async def position(self, partition):
        offset = self._offsets.get(partition)
        if offset is None:
            offset = await self.consumer.position(partition)
            self._offsets[partition] = offset
        return offset

    def highwater(self, partition):
        return self.consumer.highwater(partition)

    def last_stable_offset(self, partition):
        return self.consumer.last_stable_offset(partition)

    def last_poll_timestamp(self, partition):
        return self.consumer.last_poll_timestamp(partition)

    async def consumed(self, partition):
        highwater = self.consumer.highwater(partition)
        position = await self.position(partition)
        return highwater is not None and position is not None and \
                highwater <= position

    async def send(self, *kargs, **kwargs):
        return await self.producer.send(*kargs, **kwargs)

    async def send_and_wait(self, *kargs, **kwargs):
        """Publish a message to a topic and wait the result"""
        future = await self.producer.send(self, *kargs, **kwargs)
        return (await future)
