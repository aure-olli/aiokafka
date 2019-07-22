import asyncio

from aiokafka.consumer import AIOKafkaConsumer

loop = asyncio.get_event_loop()
consumer = AIOKafkaConsumer('test1', loop=loop)
async def test():
	await consumer.start()
	await consumer.seek_to_beginning()
	print ([await consumer.position(tp) for tp in consumer.assignment()])
	await consumer.seek_to_beginning()
	print ([await consumer.position(tp) for tp in consumer.assignment()])
loop.run_until_complete(test())
loop.run_until_complete(asyncio.sleep(1))
loop.run_until_complete(consumer.stop())
