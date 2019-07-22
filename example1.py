import time
import asyncio
import collections
loop = asyncio.get_event_loop()

from aiokafka.application import AIOKafkaApplication
from aiokafka.structs import TopicPartition

app = AIOKafkaApplication(loop=loop, auto_offset_reset='earliest', group_id='test')

# for topic in ('test1', 'test2', 'test3', 'test4', 'test5'):
# 	app.topic(topic, 'test1')

allvalues = collections.defaultdict(list)

async def merge(input, keyvalue, latency=5000):
	print ('merge', input, input.assignment)
	messages = {}
	pending = set(input.assignment)
	oldest = None
	wait = asyncio.Future()
	while True:
		# fetch data of pending partitions
		task = asyncio.ensure_future(input.getmany(*pending,
				max_records_per_partition=1, timeout_ms=None))
		await asyncio.wait((task, wait), return_when=asyncio.FIRST_COMPLETED)
		# a new message arrived
		if task.done():
			for message in (msg for l in task.result().values() for msg in l):
				tp = TopicPartition(message.topic, message.partition)
				pending.remove(tp)
				messages[tp] = (*keyvalue(message), message)
				# print ('message', keyvalue(message))
				if oldest is None or message.timestamp < oldest.timestamp:
					oldest = message
				# no pending partition, send the oldest message
			if not pending:
				# print ('not pending')
				minkey = min(k for k, _, _ in messages.values())
				for tp, v in [(tp, v) for tp, (k, v, _) in messages.items()
						if k <= minkey]:
					del messages[tp]
					pending.add(tp)
					# print ('value', v)
					allvalues[tp.partition].append(v)
					time.sleep(0.001)
				# get the new oldest
				if messages:
					oldest = min((m for _, _, m in messages.values()),
							key=lambda m: m.timestamp)
				else: oldest = None
		# finished waiting before a new message, cancel the fetch
		else: task.cancel()

		wait = None
		while wait is None:
			# print ('oldest', oldest and oldest.timestamp)
			# partitions are not consumed, so wait forever
			if not oldest or not all(await asyncio.gather(
					*(app.consumed(tp) for tp in pending))):
				wait = asyncio.Future()
				# print ('Future')
				break
			diff = (oldest.timestamp + latency) / 1000 - time.time()
			# the oldest message is not that old yet, wait for it to be older
			if diff and diff > 0:
				wait = asyncio.sleep(diff)
				# print ('sleep', diff)
				break
			# the last metadata are too old, wait for fresher metadata
			last_poll = min(app.last_poll_timestamp(tp) for tp in pending)
			if oldest.timestamp + latency > last_poll:
				wait = app.consumer.create_poll_waiter()
				# print ('create_poll_waiter')
				break
			# Send all the messages which are too old,
			# and the messages that should appear before them
			# print ('all consumed')
			minkey = min(k for k, _, _ in messages.values())
			for tp, v in [(tp, v) for tp, (k, v, _) in messages.items()
					if k <= minkey]:
				del messages[tp]
				pending.add(tp)
				# print ('value', v)
				allvalues[tp.partition].append(v)
				time.sleep(0.001)
			# get the new oldest
			if messages:
				oldest = min((m for _, _, m in messages.values()),
						key=lambda m: m.timestamp)
			else: oldest = None
			# we don't know what to wait for next

def keyvalue(message):
	value = tuple(message.value.decode('ascii').split('-')[:2])
	return (int(value[1]), value)

task = app.partition_task(merge,
	app.partitionable_consumer('test1', 'test2', 'test3', 'test4', 'test5', cache=1),
	keyvalue=keyvalue)

import asyncio

async def run():
	await app.start()
	# print ('seek_to_beginning', await app.consumer.seek_to_beginning())
	# print ('seek_to_beginning', await app.consumer.seek_to_beginning())
	# print ([await app.position(tp) for tp in app.consumer.assignment()])
	await task.start()
	async def print_allvalues():
		while True:
			await asyncio.sleep(1)
			print ('allvalues', sum(len(r) for r in allvalues.values()))
	pav = asyncio.ensure_future(print_allvalues())
	await asyncio.sleep(20)
	pav.cancel()
	await app.stop()

asyncio.get_event_loop().run_until_complete(run())


import asyncio
import traceback
unfinished = []
for task in asyncio.Task.all_tasks():
	if task.done():
		try: task.result()
		except asyncio.CancelledError: pass
		except:
			print (task)
			traceback.print_exc()
	else:
		unfinished.append(task)
		print (task)

# print (allvalues)
print (sum(len(r) for r in allvalues.values()))
print ([(p, len(r)) for p, r in allvalues.items()])
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
