import time
import asyncio
import collections
import random
import string
import traceback
loop = asyncio.get_event_loop()

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.application import Application
from aiokafka.application.tasks import AbstractTask, get_parition
from aiokafka.structs import TopicPartition

consumer = AIOKafkaConsumer('test0', 'table0', loop=loop,
		auto_offset_reset='earliest', isolation_level='read_committed')
loop.run_until_complete(consumer.start())
# time.sleep(random.uniform(5, 15))

allvalues = collections.defaultdict(list)

time_start = time.time()

length = [0, 0]
while True:
# while time.time() - time_start < 120:
	duration = random.uniform(7, 17)
	# duration = 100
	# length = sum(len(r) for r in allvalues.values())
	print ('run for', duration)
	local_loop = asyncio.new_event_loop()
	app = Application(loop=local_loop,
			auto_offset_reset='earliest', group_id='test',
			max_partition_fetch_bytes=20000,
			isolation_level='read_committed',
			transactional_id='test-'+''.join(random.choices(string.ascii_lowercase, k=8)), enable_idempotence=True)
			# transactional_id='test', enable_idempotence=True)

	async def merge(partition, input, output, table, keyvalue, latency=5000):
		try:
			print ('merge', partition, table._data)
			messages = {}
			pending = set(input.assignment)
			oldest = None
			wait = asyncio.Future()
			while True:
				# fetch data of pending partitions
				task = asyncio.ensure_future(input.getmany(*pending,
						max_records_per_partition=1, timeout_ms=None))
				await input.wait((task, wait), return_when=asyncio.FIRST_COMPLETED)
				# a new message arrived
				if task.done():
					for message in (msg for l in task.result().values() for msg in l):
						tp = TopicPartition(message.topic, message.partition)
						pending.remove(tp)
						messages[tp] = (*keyvalue(message), message)
						# print (partition, 'message', keyvalue(message))
						if oldest is None or message.timestamp < oldest.timestamp:
							oldest = message
						# no pending partition, send the oldest message
					if not pending:
						# print (partition, 'not pending')
						minkey = min(k for k, _, _ in messages.values())
						for tp, v in [(tp, v) for tp, (k, v, _) in messages.items()
								if k <= minkey]:
							del messages[tp]
							pending.add(tp)
							# print (partition, 'value', v)
							allvalues[tp.partition].append(v)
							key = ('%s-%s' % (tp.topic, tp.partition)).encode('ascii')
							value = str(int(table.get(key, b'0').decode('ascii')) + 1).encode('ascii')
							table[key] = value
							await output.send(value=v)
							# time.sleep(0.005)
						# get the new oldest
						if messages:
							oldest = min((m for _, _, m in messages.values()),
									key=lambda m: m.timestamp)
						else: oldest = None
				# finished waiting before a new message, cancel the fetch
				else:
					task.cancel()
					# try: await task
					# except asyncio.CancelledError: pass

				wait = None
				while wait is None:
					# partitions are not consumed, so wait forever
					if not oldest or not all(await asyncio.gather(
							*(app.consumed(tp) for tp in pending))):
						wait = asyncio.Future()
						# print (partition, 'Future')
						break
					diff = (oldest.timestamp + latency) / 1000 - time.time()
					# the oldest message is not that old yet, wait for it to be older
					if diff and diff > 0:
						wait = asyncio.sleep(diff)
						# print (partition, 'sleep', diff)
						break
					# the last metadata are too old, wait for fresher metadata
					last_poll = min(app.last_poll_timestamp(tp) for tp in pending)
					if oldest.timestamp + latency > last_poll:
						wait = app.consumer.create_poll_waiter()
						# print (partition, 'create_poll_waiter')
						break
					# Send all the messages which are too old,
					# and the messages that should appear before them
					# print (partition, 'all consumed')
					minkey = min(k for k, _, _ in messages.values())
					for tp, v in [(tp, v) for tp, (k, v, _) in messages.items()
							if k <= minkey]:
						del messages[tp]
						pending.add(tp)
						# print (partition, 'value', v)
						allvalues[tp.partition].append(v)
						key = ('%s-%s' % (tp.topic, tp.partition)).encode('ascii')
						value = str(int(table.get(key, b'0').decode('ascii')) + 1).encode('ascii')
						table[key] = value
						await output.send(value=v)
						# time.sleep(0.005)
					# get the new oldest
					if messages:
						oldest = min((m for _, _, m in messages.values()),
								key=lambda m: m.timestamp)
					else: oldest = None
					# we don't know what to wait for next
		except Exception as e:
			print (partition, '!!! merge except', traceback.format_exc())
			raise

	def keyvalue(message):
		value = message.value.decode('ascii').split('-')[:2]
		return (int(value[1]), '-'.join(value).encode('ascii'))

	task = app.partition_task(merge, get_parition,
		app.partitionable_consumer('test1', 'test2', 'test3', 'test4', 'test5', cache=1),
		app.partitionable_producer('test0'),
		# app.partitionable_producer('table0'),
		app.partitionable_table('table0'),
		keyvalue=keyvalue)

	import asyncio

	class PrintTask(AbstractTask):

		async def before_rebalance(self, revoked):
			print ('before_rebalance')

		async def after_rebalance(self, assigned):
			print ('after_rebalance')
			print ('allvalues', sum(len(r) for r in allvalues.values()))
			print (sorted((p, len(r)) for p, r in allvalues.items()))

		async def before_commit(self):
			print ('before_commit')

		async def after_commit(self):
			print ('after_commit')
			print ('allvalues', sum(len(r) for r in allvalues.values()))
			print (sorted((p, len(r)) for p, r in allvalues.items()))

		async def stop(self):
			print ('stop')
			print ('allvalues', sum(len(r) for r in allvalues.values()))
			print (sorted((p, len(r)) for p, r in allvalues.items()))

	async def run():
		await app.start()
		# print ('seek_to_beginning', await app.consumer.seek_to_beginning())
		# print ('seek_to_beginning', await app.consumer.seek_to_beginning())
		# print ([await app.position(tp) for tp in app.consumer.assignment()])
		app.register_task(PrintTask())
		await asyncio.sleep(0.1)
		await task.start()
		await asyncio.sleep(duration)
		# await app.stop()
		# await app.abort_transaction()

	local_loop.run_until_complete(run())
	local_loop.close()
	print ('=== closed ===')
	if length[0] == sum(len(r) for r in allvalues.values()):
		length[1] += 1
		if length[1] > 10:
			break
	else:
		length = [sum(len(r) for r in allvalues.values()), 0]
	# time.sleep(5)

unseen = set(range(50000))
allcount = {}
async def check():
	allvalues.clear()
	async for msg in consumer:
		if msg.topic == 'test0':
			value = tuple(msg.value.decode('ascii').split('-'))
			unseen.discard(int(value[1]))
			allvalues[msg.partition].append(value)
			# if sum(len(r) for r in allvalues.values()) == 5000: break
		if msg.topic == 'table0':
			allcount[msg.key.decode('ascii')] = int(msg.value.decode('ascii'))

loop.run_until_complete(asyncio.wait([check()], timeout=2))
loop.run_until_complete(consumer.stop())

# import asyncio
# import traceback
# unfinished = []
# for task in asyncio.Task.all_tasks():
# 	if task.done():
# 		try: task.result()
# 		except asyncio.CancelledError: pass
# 		except:
# 			print (task)
# 			traceback.print_exc()
# 	else:
# 		unfinished.append(task)
# 		print (task)

print (sum(len(r) for r in allvalues.values()))
print (sorted((p, len(r)) for p, r in allvalues.items()))
print (sorted(allcount.items()))
# print (unseen)
for (i, r) in allvalues.items():
	index = {}
	prev = None
	for j, v in enumerate(r):
		if v in index:
			print ('=====', 'duplicate', i, v, index[v], j)
		index[v] = j
		if j and int(v[1]) < prev:
			print ('=====', 'unordered', i, prev, v, j-1, j)
		prev = int(v[1])
