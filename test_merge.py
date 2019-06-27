import collections
import itertools
import logging
import abc
import time
import asyncio

from kafka.vendor import six

from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.common import TopicPartition
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.producer import AIOKafkaProducer
from aiokafka.abc import ConsumerRebalanceListener
import aiokafka.errors as Errors

log = logging.getLogger(__name__)


class StreamMergeAssignor(AbstractPartitionAssignor):
	"""
	A round robin assignator that keeps the same partition numbers
	of different streams with the sane client
	Deeply inspired from aiokafka.RoundRobinAssignator
	"""
	name = 'streammerge'
	version = 0

	@classmethod
	def assign(cls, cluster, member_metadata):
		# get all topics and check every memeber has the same
		all_topics = None
		for metadata in six.itervalues(member_metadata):
			if all_topics is None:
				all_topics = set(metadata.subscription)
			elif all_topics != set(metadata.subscription):
				diff = all_topics.symmetric_difference(metadata.subscription)
				raise RuntimeError('Topic(s) %s do not appear in all members',
					', '.join(diff))
		# get all partition numbers and check every topic has the same
		all_partitions = None
		for topic in all_topics:
			partitions = cluster.partitions_for_topic(topic)
			if partitions is None:
				raise RuntimeError('No partition metadata for topic %s', topic)
			if all_partitions is None:
				all_partitions = set(partitions)
			elif all_partitions != set(partitions):
				diff = all_partitions.symmetric_difference(partitions)
				raise RuntimeError('Partition(s) %s do not appear in all topics',
					', '.join(str(p) for p in diff))
		all_partitions = sorted(all_partitions)

		assignment = collections.defaultdict(lambda: collections.defaultdict(list))
		# round robin assignation of the partition numbers
		member_iter = itertools.cycle(sorted(member_metadata.keys()))
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


class AbstractStreamMerger:
	"""
	The class to treat messages from different topics
	"""

	@abc.abstractmethod
	def __init__(self, topics):
		...

	@abc.abstractmethod
	async def feed(self, msg):
		"""
		Receives a new message.
		Returns a ``({partition: offset}, timestamp)`` tuple
		with a dictionnary of offsets updates,
		and the timestamp of the oldest message in store
		"""
		...

	@abc.abstractmethod
	async def wakeup(self):
		"""
		Wakesup because the oldest message should be sent
		Returns a ``({partition: offset}, timestamp)`` tuple
		with a dictionnary of offsets updates,
		and the timestamp of the oldest message in store
		"""
		...

class StreamMerger(AbstractStreamMerger):
	"""
	A more advance merger
	"""

	def __init__(self, topics):
		self._pending = set(topics)
		self._messages = {}
		self._mintime = None

	@abc.abstractmethod
	async def process(self, msg):
		"""
		Pocesses a message than needs to be send
		"""
		...

	@abc.abstractmethod
	async def lowest(self, messages):
		"""
		Finds the lowest(s) messages of the dict `messages`
		Returns a non empty ordered list of `messages` keys to process
		"""
		...

	@abc.abstractmethod
	async def lower(self, messages, msg):
		"""
		Finds all messages of the dict `messages` lower than `msg`
		Returns an ordered list of `messages` keys to process
		"""
		...

	async def _update(self, topics):
		offsets = {}
		for topic in topics:
			msg = self._messages.pop(topic)
			await self.process(msg)
			offsets[topic] = msg.offset + 1
			self._pending.add(topic)
			if self._mintime and self._mintime[1] == topic:
				self._mintime = None
		if not self._mintime:
			for topic, msg in self._messages.items():
				if not self._mintime or self._mintime[0] > msg.timestamp:
					self._mintime = (msg.timestamp, topic)
		return offsets

	async def feed(self, msg):
		self._messages[msg.topic] = msg
		self._pending.discard(msg.topic)
		if not self._mintime or self._mintime[0] > msg.timestamp:
			self._mintime = (msg.timestamp, msg.topic)
		if not self._pending:
			topics = await self.lowest(self._messages) or ()
			offsets = await self._update(topics)
		else: offsets = {}
		return offsets, self._mintime and self._mintime[0]

	async def wakeup(self):
		# print('@@@@@', 'wakeup', self._mintime, time.time())
		msg = self._messages[self._mintime[1]]
		topics = await self.lower(self._messages, msg) or ()
		if self._mintime[1] not in topics:
			topics = list(topics)
			topics.append(self._mintime[1])
		offsets = await self._update(topics)
		return offsets, self._mintime and self._mintime[0]


class StreamSorter(StreamMerger):
	"""
	A meger that only requires a key method for sorting
	"""

	def __init__(self, topics):
		super().__init__(topics)
		self._keys = {}

	async def key(self, msg):
		"""
		Returns the sorting key of `msg`
		"""
		return await msg.timestamp

	async def feed(self, msg):
		self._keys[msg.topic] = await self.key(msg)
		return await super().feed(msg)

	async def _update(self, topics):
		for topic in topics:
			self._keys.pop(topic)
		return await super()._update(topics)

	async def lowest(self, messages):
		lowest, lkey = None, None
		for topic in messages:
			key = self._keys[topic]
			if not lowest or key < lkey:
				lowest = [topic]
				lkey = key
			elif key <= lkey:
				lowest.append(topic)
		return lowest

	async def lower(self, messages, oldest):
		lower, lkey = [], self._keys[oldest.topic]
		for topic in messages:
			if self._keys[topic] <= lkey:
				lower.append(topic)
		lower.sort(key=lambda t: self._keys[t])
		return lower


class MergeConsumerRebalanceListener(ConsumerRebalanceListener):
	"""
	A `ConsumerRebalanceListener` for committing offsets
	and rebuilding processors
	"""
	def __init__(self, consumer):
		self._consumer = consumer

	async def on_partitions_revoked(self, revoked):
		if not self._consumer._merge_manager: return
		await self._consumer._merge_manager.clean_processors()

	async def on_partitions_assigned(self, revoked):
		if not self._consumer._merge_manager: return
		self._consumer._merge_manager.build_processors()

class MergeManager:
	"""
	An helping class that manages reading, the processors and the committing
	"""
	def __init__(self, consumer, subscriptions, loop,
			processor_factory,
			processor_latency_ms=5000,
			enable_auto_commit=True,
			auto_commit_interval_ms=5000,
			retry_backoff_ms=100):
		self._subscriptions = subscriptions
		self._consumer = consumer
		self._loop = loop
		self._processor_latency_ms = processor_latency_ms
		self._processor_factory = processor_factory
		self._enable_auto_commit = enable_auto_commit
		self._auto_commit_interval_ms = auto_commit_interval_ms
		self._retry_backoff_ms = retry_backoff_ms
		self._next_autocommit_deadline = \
			loop.time() + auto_commit_interval_ms / 1000
		self._pending_tp = {}
		self._proceessors = {}
		self._wakeups = {}
		self._offsets = {}
		self._pending_tasks = ()
		self._closing = asyncio.Future(loop=loop)
		self._rebalancing = None
		self._merge_task = None
		self.build_processors()

	async def close(self):
		"""
		Ends the mege task if running, and do the last commit
		"""
		print ('MergeManager', 'close', self._closing.done())
		if self._closing.done(): return
		self._closing.set_result(None)
		if self._rebalancing:
			self._rebalancing.cancel()
			self._rebalancing = None
		for task in self._pending_tasks:
			task.cancel()
			await task
		if self._merge_task:
			await self._merge_task
		try: await self._maybe_do_last_autocommit()
		except Errors.KafkaError as err:
			# We did all we could, all we can is show this to user
			log.error("Failed to commit on finallization: %s", err)
		self._consumer = None # avoid memory leak

	async def _readone(self):
		"""
		A coroutine to read one message among the pending partitions
		"""
		try:
			# print ('_readone', 'getone', len(self._pending_tp))
			# print ('_readone', 'getone', time.time(), self._pending_tp)
			msg = await self._consumer.getone(*self._pending_tp)
			# print ('_readone', 'getone', msg.topic, msg.partition, self._pending_tp)
			# print ('_readone', msg.topic, msg.partition, msg.offset)
			partition = msg.partition
			tp = TopicPartition(msg.topic, partition)
			# self._offsets[tp] = msg.offset
			self._pending_tp.remove(tp)
			offsets, timestamp = await self._proceessors[partition].feed(msg)
			self._update_partition(partition, offsets, timestamp)
		except asyncio.CancelledError: pass

	async def _sleep(self):
		"""
		A coroutine to sleep and wake up the processors
		"""
		try:
			assignment = self._subscriptions.subscription.assignment
			# sort the wakeup timestamps
			wakeups = sorted(self._wakeups.items(), key=lambda t: t[1])
			# wait for them in this particular order
			for partition, timestamp in wakeups:
				timeout = (timestamp + self._processor_latency_ms) / 1000 - \
						time.time()
				# print ('_sleep', 'timestamp', timeout)
				if timeout > 0: await asyncio.sleep(timeout)
				# print ('_sleep', 'timestamp', 'finished')
				# check that all the pending partitions with the same number are consumed
				for tp in self._pending_tp:
					if tp.partition != partition: continue
					state = assignment.state_value(tp)
					offset = self._offsets.get(tp, state.position)
					# print ('_sleep', 'timestamp', state.timestamp, timestamp, 'highwater', offset, state.highwater)
					if state.timestamp < timestamp + \
						self._processor_latency_ms or state.highwater > offset:
						self._wakeups.pop(partition)
						break
				# wakeup the processor
				else:
					offsets, timestamp = \
						await self._proceessors[partition].wakeup()
					self._update_partition(partition, offsets, timestamp)
					return
			# nothing wakeup left, just wait for cancel
			while True: await asyncio.sleep(100)
		except asyncio.CancelledError: pass

	def _update_partition(self, partition, offsets, timestamp):
		for topic, offset in offsets.items():
			tp = TopicPartition(topic, partition)
			self._pending_tp.add(tp)
			self._offsets[tp] = offset
		if timestamp:
			self._wakeups[partition] = timestamp
		else: self._wakeups.pop(partition, None)

	async def _merge_routine(self):
		"""
		The main routine, reading data and committing offsets
		"""
		try:
			# init the rocessors
			self.build_processors()

			while not self._closing.done():

				if self._rebalancing: await self._rebalancing
				# time to wait for the next autocommit
				wait_timeout = await self._maybe_do_autocommit()
				# two concurrent tasks: read data and wakeup processors
				if not self._pending_tasks:
					self._pending_tasks = [
						asyncio.ensure_future(self._sleep(), loop=self._loop),
						asyncio.ensure_future(self._readone(), loop=self._loop),
					]
				try:
					done, self._pending_tasks = await asyncio.wait(self._pending_tasks,
							return_when=asyncio.FIRST_COMPLETED,
							timeout=wait_timeout, loop=self._loop)
				# maybe cancelled by rebalancing, continue
				except asyncio.CancelledError: continue
				# first done wins, other is cancelled
				if done:
					for task in self._pending_tasks:
						task.cancel()
						await task
					self._pending_tasks = ()

		except asyncio.CancelledError:
			pass
		except Exception:
			log.error("Unexpected error in merge routine", exc_info=True)
			raise Errors.KafkaError("Unexpected error during merge")

	def build_processors(self):
		"""
		builds the processors and other data structures
		"""
		if self._closing.done(): return
		self._wakeups = {}
		self._offsets = {}
		self._pending_tp = set(self._consumer.assignment())

		assignment = self._subscriptions.subscription.assignment
		partitions = collections.defaultdict(list)
		for partition in self._pending_tp:
			state = assignment.state_value(partition)
			if state.has_valid_position:
				self._offsets[partition] = state.position
			partitions[partition.partition].append(partition.topic)
		print('build_processors', 'offsets', self._offsets)

		self._proceessors = {p: self._processor_factory(ts)
				for p, ts in partitions.items()}

		if self._rebalancing:
			self._rebalancing.set_result(None)
			self._rebalancing = None

	async def clean_processors(self):
		"""
		cleans the processors and other data structures, do a last commit
		"""
		for task in self._pending_tasks:
			task.cancel()
			await task
		try: await self._maybe_do_last_autocommit()
		except Errors.KafkaError as err:
			# We did all we could, all we can is show this to user
			log.error("Failed to commit on finallization: %s", err)

		self._pending_tp = {}
		self._proceessors = {}
		self._wakeups = {}
		self._offsets = {}
		self._pending_tasks = ()
		self._rebalancing = asyncio.Future(loop=loop)

	async def _maybe_do_autocommit(self):
		if not self._enable_auto_commit:
			return None
		now = self._loop.time()
		interval = self._auto_commit_interval_ms / 1000
		backoff = self._retry_backoff_ms / 1000
		if now > self._next_autocommit_deadline:
			if self._offsets:
				print ('_maybe_do_autocommit', self._offsets)
				try: await self._consumer.commit(self._offsets)
				except Errors.KafkaError as error:
					log.warning("Auto offset commit failed: %s", error)
					if error.retriable:
						# Retry after backoff.
						self._next_autocommit_deadline = \
							self._loop.time() + backoff
						return backoff
					else:
						raise
			# If we had an unrecoverable error we expect the user to handle it
			# from another source (say Fetcher, like authorization errors).
			self._next_autocommit_deadline = now + interval

		return max(0, self._next_autocommit_deadline - self._loop.time())

	async def _maybe_do_last_autocommit(self):
		if not self._enable_auto_commit or not self._offsets:
			return
		print ('_maybe_do_last_autocommit', self._offsets)
		await self._consumer.commit(self._offsets)
		self._offsets = {}

	def run(self):
		"""
		Returns the main coroutine
		"""
		if not self._merge_task:
			self._merge_task = asyncio.ensure_future(
					self._merge_routine(), loop=loop)
		return self._merge_task


class MergeConsumer(AIOKafkaConsumer):
	"""
	A consumer specialized for merging tasks
	"""
	def __init__(self, *topics,
				processor_factory,
				processor_latency_ms=5000,
				enable_auto_commit=True,
				partition_assignment_strategy=(StreamMergeAssignor,),
				**kwargs):
		super().__init__(
				enable_auto_commit=False,
				partition_assignment_strategy=partition_assignment_strategy,
				**kwargs)
		self._enable_auto_commit_ = self._group_id is not None and enable_auto_commit
		self._processor_factory = processor_factory
		self._processor_latency_ms = processor_latency_ms
		self._merge_manager = None
		self.subscribe(topics, listener=MergeConsumerRebalanceListener(self))

	async def start(self):
		await super().start()
		self._merge_manager = MergeManager(
			self, self._subscription, loop=self._loop,
			processor_factory=self._processor_factory,
			processor_latency_ms=self._processor_latency_ms,
			enable_auto_commit=self._enable_auto_commit_,
			auto_commit_interval_ms=self._auto_commit_interval_ms,
			retry_backoff_ms=self._retry_backoff_ms)

	async def stop(self):
		if self._closed:
			return
		log.debug("Closing the KafkaConsumer.")
		self._closed = True
		if self._merge_manager:
			await self._merge_manager.close()
		if self._coordinator:
			await self._coordinator.close()
		if self._fetcher:
			await self._fetcher.close()
		await self._client.close()
		log.debug("The KafkaConsumer has closed.")

	def run(self):
		return self._merge_manager.run()

"""
Requirements:
	kafka with topics test1...test5 created (or auto create)

for i in {1..5}; do bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic "test$i"; done
for i in {1..5}; do bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic "test$i"; done

Usage:
	python test_merge.py fill: fills kafka topics with data
	python test_merge.py: runs the merger from the beginning of the topics
	python test_merge.py slave: runs the merger from the current offsets
"""
if __name__ == "__main__":
	import sys

	results = {}

	loop = asyncio.get_event_loop()
	topics = ('test1', 'test2', 'test3', 'test4', 'test5')

	if len(sys.argv) >= 2 and sys.argv[1] == 'fill':
		producer = AIOKafkaProducer(loop=loop)
		loop.run_until_complete(producer.start())
		for i in range(5000):
			for topic in topics:
				# data is topic-i-xxxxx...  in order to make long messages that needs to be polled in several requests
				loop.run_until_complete(producer.send(topic, '-'.join((topic, str(i), 1000*'x')).encode('ascii')))
		loop.run_until_complete(producer.stop())

	else:

		class Test(StreamSorter):

			async def key(self, msg):
				return int(msg.value.decode('ascii').split('-')[1])

			async def process(self, msg):
				time.sleep(0.001) # slow down reading
				value = tuple(msg.value.decode('ascii').split('-')[:2])
				results.setdefault(msg.partition, []).append(value)
				# print('process', msg.partition, value)

		for i in range(10):
			consumer = MergeConsumer(*topics, loop=loop, processor_factory=Test, group_id='mygroup')
			loop.run_until_complete(consumer.start())

			if not i:
				if len(sys.argv) < 2 or sys.argv[1] != 'slave':
					print ('seek_to_beginning')
					loop.run_until_complete(consumer.seek_to_beginning())

			try: loop.run_until_complete(asyncio.shield(
				asyncio.wait_for(consumer.run(), timeout=12, loop=loop)))
			except asyncio.TimeoutError: pass
			loop.run_until_complete(consumer.stop())
			loop.run_until_complete(asyncio.gather(*asyncio.Task.all_tasks()))

			print ('results', sum(len(r) for r in results.values()))
			diff = False
			for (i, r) in results.items():
				index = {}
				prev = None
				for j, v in enumerate(r):
					if v in index:
						print ('duplicate', i, v, index[v], j)
					index[v] = j
					if j and int(v[1]) < prev:
						print ('unordered', i, prev, v, j-1, j)
					prev = int(v[1])
			if diff or sum(len(r) for r in results.values()) >= 25000: break
