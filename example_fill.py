import time
import asyncio
import random
loop = asyncio.get_event_loop()

from aiokafka.producer import AIOKafkaProducer

topics = ('test1', 'test2', 'test3', 'test4', 'test5')
producer = AIOKafkaProducer(loop=loop, max_request_size=10000)
loop.run_until_complete(producer.start())
for i in range(5000):
	if i % 100 == 0:
		print (i)
	topic = random.choice(topics)
	loop.run_until_complete(producer.send(topic, '-'.join((topic, str(i), 1000*'x')).encode('ascii')))
	time.sleep(0.005)
loop.run_until_complete(producer.stop())
