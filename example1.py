import time
import asyncio
loop = asyncio.get_event_loop()

from aiokafka.application import AIOKafkaApplication
from aiokafka.structs import TopicPartition

app = AIOKafkaApplication(loop=loop)

for topic in ('test1', 'test2', 'test3', 'test4', 'test5'):
	app.topic(topic, 'test1')

async def merge(input, keyvalue, latency=5000):
	print ('merge', input, input.assignment())
	print ('merge', '0')
	messages = {}
	pending = set(input.assignment())
	oldest = None
	wait = asyncio.Future()
	print ('merge', '10')
	while True:
		print ('merge', '20')
		# fetch data of pending partitions
		task = asyncio.ensure_future(input.getone(*pending))
		print ('task', task)
		asyncio.wait((task, wait), return_when=asyncio.FIRST_COMPLETED)
		# a new message arrived
		print ('task.done()', task.done())
		if task.done():
			message = task.result()
			tp = TopicPartition(message.topic, message.partition)
			pending.remove(tp)
			messages[tp] = (*keyvalue(message), message)
			# no pending partition, send the oldest message
			if not pending:
				minkey = min(k for k, _, _ in messages.values())
				for tp, v in [(tp, v) for tp, (k, v, _) in messages.items()
						if k <= miney]:
					del messages[tp]
					pending.add(tp)
					print ('value', v)
				# get the new oldest
				if messages:
					oldest = min((m for _, _, m in messages.values()),
							key=lambda m: m.timestamp)
				else: oldest = None
		# finished waiting before a new message, cancel the fetch
		else: task.cancel()

		wait = None
		while wait is None:
			# partitions are not consumed, so wait forever
			if not oldest or not all(input.consumed(tp) for tp in pending):
				wait = asyncio.Future()
				break
			diff = (oldest.timestamp + latency) / 1000 - time.time()
			print ('diff', diff, diff and diff > 0)
			# the oldest message is not that old yet, wait for it to be older
			if diff and diff > 0:
				wait = asyncio.sleep(diff)
				break
			# the last metadata are too old, wait for fresher metadata
			last_poll = min(input.last_poll_timestamp(tp)
					for tp in pending) - latency
			if oldest.timestamp > last_poll:
				wait = app.next_poll()
				break
			# Send all the messages which are too old,
			# and the messages that should appear before them
			maxkey = max(k for k, _, m in messages.values()
					if m.timestamp <= last_poll)
			for tp, _, v in sorted(
					((tp, k, v) for tp, (k, v, _) in messages.values()
					if k <= maxkey), key=lambda t: t[1]):
				del messages[tp]
				pending.add(tp)
				print ('value', v)
			# get the new oldest
			if messages:
				oldest = min((m for _, _, m in messages.values()),
						key=lambda m: m.timestamp)
			else: oldest = None
			# we don't know what to wait for next

def keyvalue(message):
	value = message.value.decode('ascii').split('-')[:2]
	return (int(value[1]), value)

task = app.partition_task(merge,
	app.stream('test1', 'test2', 'test3', 'test4', 'test5'),
	keyvalue=keyvalue)

await app.start()
# await task.run()




await asyncio.sleep(2)


for task in asyncio.Task.all_tasks():
	if task.done(): task.result()
