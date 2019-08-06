import time
import asyncio
import collections
import random
import string
loop = asyncio.get_event_loop()

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.application.tasks import AbstractTask, get_parition
from aiokafka.structs import TopicPartition

consumer = AIOKafkaConsumer('test0', loop=loop,
		auto_offset_reset='earliest', isolation_level='read_committed')
loop.run_until_complete(consumer.start())
# time.sleep(random.uniform(5, 15))

allvalues = collections.defaultdict(list)

unseen = set(range(50000))
async def check():
	allvalues.clear()
	async for msg in consumer:
		value = tuple(msg.value.decode('ascii').split('-'))
		unseen.discard(int(value[1]))
		allvalues[msg.partition].append(value)
		# if sum(len(r) for r in allvalues.values()) == 5000: break

loop.run_until_complete(asyncio.wait([check()], timeout=5))
loop.run_until_complete(consumer.stop())

print (sum(len(r) for r in allvalues.values()))
print (sorted((p, len(r)) for p, r in allvalues.items()))
print (unseen)
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
