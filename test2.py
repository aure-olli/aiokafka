    import asyncio

    available_data = []
    data_ready = asyncio.Future()

    def feed_data(data):
        global data_ready
        available_data.append(data)
        data_ready.set_result(None)
        data_ready = asyncio.Future()

    async def consume_data():
        while not available_data:
            await asyncio.shield(data_ready)
        return available_data.pop()

    async def wrapped_consumer():
        task = asyncio.ensure_future(consume_data())
        return await task

    async def wrapped_consumer():
        task = asyncio.ensure_future(consume_data())
        try:
            await asyncio.wait([task, stop_future])
        finally:
            task.cancel()
            await asyncio.wait([task])
        if not task.cancelled():
            return task.result()
        else:
            raise RuntimeError('stopped')

    async def test():
        task = asyncio.ensure_future(wrapped_consumer())
        await asyncio.sleep(0)
        feed_data('data')
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        print ('task', task)
        print ('available_data', available_data)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
