import time
import asyncio
import collections
import random
import string
import itertools
loop = asyncio.get_event_loop()

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.application.tasks import AbstractTask, get_parition
from aiokafka.structs import TopicPartition

consumer = AIOKafkaConsumer('test0', 'table0', loop=loop,
		auto_offset_reset='earliest', isolation_level='read_committed')
loop.run_until_complete(consumer.start())
# time.sleep(random.uniform(5, 15))

allvalues = collections.defaultdict(list)

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
			allcount[tuple(msg.key.decode('ascii').split('-'))] = int(msg.value.decode('ascii'))

loop.run_until_complete(asyncio.wait([check()], timeout=2))
loop.run_until_complete(consumer.stop())

print (sum(len(r) for r in allvalues.values()))
print (sorted((p, len(r)) for p, r in allvalues.items()))
allcount = sorted(allcount.items())
# print (allcount)
print ([(int(k), sum(int(t[1]) for t in g)) for k, g in itertools.groupby(sorted(allcount, key=lambda t: t[0][1]), key=lambda t: t[0][1])])
print (list(unseen)[:10])
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
