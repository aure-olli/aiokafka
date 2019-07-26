import asyncio
loop = asyncio.get_event_loop()
from aiokafka.consumer import AIOKafkaConsumer
consumer = AIOKafkaConsumer('test1', 'test2', 'test3', 'test4', 'test5', loop=loop, isolation_level='read_committed', auto_offset_reset='earliest')

async def test():
	await consumer.start()
	async for msg in consumer:
		print ((msg.topic, msg.partition, msg.value[:12], msg.offset))
loop.run_until_complete(test())
