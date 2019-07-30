import asyncio
import syncio

loop = asyncio.get_event_loop()
fut = syncio.SyncFuture()

async def wait():
    await fut

task = syncio.ensure_sync_future(wait())

async def test():
    await asyncio.sleep(0)
    fut.set_result(None)
    task.cancel()
    await asyncio.sleep(0)
    print ('fut', fut)
    print ('task', task)

loop.run_until_complete(test())






import asyncio
import syncio

loop = asyncio.get_event_loop()

data = []
fut_data = syncio.SyncFuture()

async def get_data():
    while not data:
        await syncio.sync_shield(fut_data)
    return data.pop()

fut_wapper = asyncio.Future()

async def wrapper_data():
    task = syncio.ensure_sync_future(get_data())
    return await task

async def test():
    task = asyncio.ensure_future(wrapper_data())
    await asyncio.sleep(0)
    data.append('data')
    fut_data.set_result(None)
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    print ('task', task)
    print ('data', data)

loop.run_until_complete(test())






import asyncio
from aiokafka.application import syncio

loop = asyncio.get_event_loop()

data = []
fut_data = asyncio.Future()

async def get_data():
    while not data:
        await asyncio.shield(fut_data)
    return data.pop()

fut_wapper = asyncio.Future()

async def wrapper_data():
    task = syncio.ensure_sync_future(get_data())
    try:
        await syncio.sync_wait([task, fut_wapper], return_when=asyncio.FIRST_COMPLETED)
    finally:
        task.cancel()
    if task.done():
        return task.result()

async def test():
    task = asyncio.ensure_future(wrapper_data())
    await asyncio.sleep(0)
    data.append('data')
    fut_data.set_result(None)
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    print ('task', task)
    print ('data', data)
    print ('result', task.result())

loop.run_until_complete(test())





import asyncio
loop = asyncio.get_event_loop()

data = []
fut_data = asyncio.Future()

async def get_data():
    while not data:
        await asyncio.shield(fut_data)
    return data.pop()

fut_wapper = asyncio.Future()

async def wrapper_data():
    task = asyncio.ensure_future(get_data())
    # task = fut_data
    cancelled = False
    def cb(_):
        nonlocal cancelled
        if not task.done():
            cancelled = True
            task.cancel()
    fut_wapper.add_done_callback(cb)
    try:
        await task
    except asyncio.CancelledError:
        if not cancelled:
            raise
    finally:
        print ('task', task)
        fut_wapper.remove_done_callback(cb)
        if not task.done():
            task.cancel()
    if not task.cancelled():
        return task.result()
    else:
        # do something else
        pass

async def test():
    task = asyncio.ensure_future(wrapper_data())
    await asyncio.sleep(0)
    data.append('data')
    fut_data.set_result(None)
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    print ('wrapper_data', task)
    print ('data', data)

loop.run_until_complete(test())



import asyncio

available_data = []
data_ready = asyncio.Future()

def feed_data(data):
    nonlocal data_ready
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
