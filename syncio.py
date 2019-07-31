import functools

from asyncio import events
from asyncio import coroutines
from asyncio import futures
from asyncio import tasks

__all__ = (
    'SyncFuture',
    'SyncTask',
    'ensure_sync_future',
    'sync_wait',
    'sync_wait_for',
    'sync_gather',
)


class SyncFuture(futures._PyFuture):

    ### callbacks are called now instead of being scheduled ###
    def _schedule_callbacks(self):
        """Internal: Ask the event loop to call all callbacks.
        The callbacks are scheduled to be called as soon as possible. Also
        clears the callback list.
        """
        callbacks = self._callbacks[:]
        if not callbacks:
            return

        self._callbacks[:] = []
        callbacks.reverse()
        for callback in callbacks:
            handle = events.Handle(callback, [self], self._loop)
            handle._run()


### Inherits from SyncFuture for its callback mechanism ###
class SyncTask(tasks._PyTask, SyncFuture):

    def _step(self, exc=None):
        assert not self.done(), \
            '_step(): already done: {!r}, {!r}'.format(self, exc)
        if self._must_cancel:
            if not isinstance(exc, futures.CancelledError):
                exc = futures.CancelledError()
            self._must_cancel = False
        coro = self._coro
        self._fut_waiter = None
        self.__class__._current_tasks[self._loop] = self
        # Call either coro.throw(exc) or coro.send(None).
        try:
            if exc is None:
                # We use the `send` method directly, because coroutines
                # don't have `__iter__` and `__next__` methods.
                result = coro.send(None)
            else:
                result = coro.throw(exc)
        except StopIteration as exc:
            if self._must_cancel:
                # Task is cancelled right before coro stops.
                self._must_cancel = False
                self.set_exception(futures.CancelledError())
            else:
                self.set_result(exc.value)
        except futures.CancelledError:
            ### be sure to call SyncFuture.cancel ###
            super(tasks._PyTask, self).cancel()  # I.e., Future.cancel(self).
        except Exception as exc:
            self.set_exception(exc)
        except BaseException as exc:
            self.set_exception(exc)
            raise
        else:
            blocking = getattr(result, '_asyncio_future_blocking', None)
            if blocking is not None:
                # Yielded Future must come from Future.__iter__().
                if result._loop is not self._loop:
                    self._loop.call_soon(
                        self._step,
                        RuntimeError(
                            'Task {!r} got Future {!r} attached to a '
                            'different loop'.format(self, result)))
                elif blocking:
                    if result is self:
                        self._loop.call_soon(
                            self._step,
                            RuntimeError(
                                'Task cannot await on itself: {!r}'.format(
                                    self)))
                    else:
                        result._asyncio_future_blocking = False
                        result.add_done_callback(self._wakeup)
                        self._fut_waiter = result
                        if self._must_cancel:
                            if self._fut_waiter.cancel():
                                self._must_cancel = False
                else:
                    self._loop.call_soon(
                        self._step,
                        RuntimeError(
                            'yield was used instead of yield from '
                            'in task {!r} with {!r}'.format(self, result)))
            elif result is None:
                # Bare yield relinquishes control for one event loop iteration.
                self._loop.call_soon(self._step)
                # self._step()
            elif inspect.isgenerator(result):
                # Yielding a generator is just wrong.
                self._loop.call_soon(
                    self._step,
                    RuntimeError(
                        'yield was used instead of yield from for '
                        'generator in task {!r} with {!r}'.format(
                            self, result)))
            else:
                # Yielding something else is an error.
                self._loop.call_soon(
                    self._step,
                    RuntimeError(
                        'Task got bad yield: {!r}'.format(result)))
        finally:
            ### fix KeyError when using SyncFuture ###
            self.__class__._current_tasks.pop(self._loop, None)
            self = None  # Needed to break cycles when an exception occurs.

### patch tasks._PyTask too ##
tasks._PyTask._step = SyncTask._step


def ensure_sync_future(coro_or_future, *, loop=None):
    """Wrap a coroutine or an awaitable in a sync future.
    If the argument is a Future, it is returned directly.
    """
    if coroutines.iscoroutine(coro_or_future):
        if loop is None:
            loop = events.get_event_loop()
        ### use SyncTask instead of loop.ensure_future ###
        task = SyncTask(coro_or_future, loop=loop)
        if task._source_traceback:
            del task._source_traceback[-1]
        return task
    else:
        return tasks.ensure_future(coro_or_future, loop=loop)


async def sync_wait(fs, *, loop=None, timeout=None, return_when=tasks.ALL_COMPLETED):
    """Wait for the Futures and coroutines given by fs to complete.
    The sequence futures must not be empty.
    Coroutines will be wrapped in Tasks.
    Returns two sets of Future: (done, pending).
    Usage:
        done, pending = yield from asyncio.wait(fs)
    Note: This does not raise TimeoutError! Futures that aren't done
    when the timeout occurs are returned in the second set.
    """
    if futures.isfuture(fs) or coroutines.iscoroutine(fs):
        raise TypeError("expect a list of futures, not %s" % type(fs).__name__)
    if not fs:
        raise ValueError('Set of coroutines/Futures is empty.')
    if return_when not in (tasks.FIRST_COMPLETED, tasks.FIRST_EXCEPTION, tasks.ALL_COMPLETED):
        raise ValueError('Invalid return_when value: {}'.format(return_when))

    if loop is None:
        loop = events.get_event_loop()

    ### use ensure_sync_future instead of ensure_future ###
    fs = {ensure_sync_future(f, loop=loop) for f in set(fs)}

    return await _sync_wait(fs, timeout, return_when, loop)


def _release_waiter(waiter, *args):
    if not waiter.done():
        waiter.set_result(None)


async def sync_wait_for(fut, timeout, *, loop=None):
    """Wait for the single Future or coroutine to complete, with timeout.
    Coroutine will be wrapped in Task.
    Returns result of the Future or coroutine.  When a timeout occurs,
    it cancels the task and raises TimeoutError.  To avoid the task
    cancellation, wrap it in shield().
    If the wait is cancelled, the task is also cancelled.
    This function is a coroutine.
    """
    if loop is None:
        loop = events.get_event_loop()

    if timeout is None:
        return await fut

    waiter = SyncFuture(loop=loop)
    timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    cb = functools.partial(_release_waiter, waiter)

    ### use ensure_sync_future instead of ensure_future ###
    fut = ensure_sync_future(fut, loop=loop)
    fut.add_done_callback(cb)

    try:
        # wait until the future completes or the timeout
        try:
            await waiter
        except futures.CancelledError:
            fut.remove_done_callback(cb)
            fut.cancel()
            raise

        if fut.done():
            return fut.result()
        else:
            fut.remove_done_callback(cb)
            fut.cancel()
            raise futures.TimeoutError()
    finally:
        timeout_handle.cancel()


async def _sync_wait(fs, timeout, return_when, loop):
    """Internal helper for wait() and wait_for().
    The fs argument must be a collection of Futures.
    """
    assert fs, 'Set of Futures is empty.'
    ### use SyncFuture instead of loop.create_future ###
    waiter = SyncFuture(loop=loop)
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    counter = len(fs)

    def _on_completion(f):
        nonlocal counter
        counter -= 1
        if (counter <= 0 or
            return_when == tasks.FIRST_COMPLETED or
            return_when == tasks.FIRST_EXCEPTION and (not f.cancelled() and
                                                f.exception() is not None)):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

    for f in fs:
        f.add_done_callback(_on_completion)

    try:
        await waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()

    done, pending = set(), set()
    for f in fs:
        f.remove_done_callback(_on_completion)
        if f.done():
            done.add(f)
        else:
            pending.add(f)
    return done, pending


### inherits from SyncFuture ###
class _GatheringFuture(SyncFuture):
    """Helper for gather().
    This overrides cancel() to cancel all the children and act more
    like Task.cancel(), which doesn't immediately mark itself as
    cancelled.
    """

    def __init__(self, children, *, loop=None):
        super().__init__(loop=loop)
        self._children = children
        self._cancel_requested = False

    def cancel(self):
        if self.done():
            return False
        ret = False
        for child in self._children:
            if child.cancel():
                ret = True
        if ret:
            # If any child tasks were actually cancelled, we should
            # propagate the cancellation request regardless of
            # *return_exceptions* argument.  See issue 32684.
            self._cancel_requested = True
        return ret


def sync_gather(*coros_or_futures, loop=None, return_exceptions=False):
    """Return a future aggregating results from the given coroutines
    or futures.
    Coroutines will be wrapped in a future and scheduled in the event
    loop. They will not necessarily be scheduled in the same order as
    passed in.
    All futures must share the same event loop.  If all the tasks are
    done successfully, the returned future's result is the list of
    results (in the order of the original sequence, not necessarily
    the order of results arrival).  If *return_exceptions* is True,
    exceptions in the tasks are treated the same as successful
    results, and gathered in the result list; otherwise, the first
    raised exception will be immediately propagated to the returned
    future.
    Cancellation: if the outer Future is cancelled, all children (that
    have not completed yet) are also cancelled.  If any child is
    cancelled, this is treated as if it raised CancelledError --
    the outer Future is *not* cancelled in this case.  (This is to
    prevent the cancellation of one child to cause other children to
    be cancelled.)
    """
    if not coros_or_futures:
        if loop is None:
            loop = events.get_event_loop()
        ### use SyncFuture instead of loop.create_future ###
        outer = SyncFuture(loop=loop)
        outer.set_result([])
        return outer

    arg_to_fut = {}
    for arg in set(coros_or_futures):
        if not futures.isfuture(arg):
            ### use ensure_sync_future instead of ensure_future ###
            fut = ensure_sync_future(arg, loop=loop)
            if loop is None:
                loop = fut._loop
            # The caller cannot control this future, the "destroy pending task"
            # warning should not be emitted.
            fut._log_destroy_pending = False
        else:
            fut = arg
            if loop is None:
                loop = fut._loop
            elif fut._loop is not loop:
                raise ValueError("futures are tied to different event loops")
        arg_to_fut[arg] = fut

    children = [arg_to_fut[arg] for arg in coros_or_futures]
    nchildren = len(children)
    outer = _GatheringFuture(children, loop=loop)
    nfinished = 0
    results = [None] * nchildren

    def _done_callback(i, fut):
        nonlocal nfinished
        if outer.done():
            if not fut.cancelled():
                # Mark exception retrieved.
                fut.exception()
            return

        if fut.cancelled():
            res = futures.CancelledError()
            if not return_exceptions:
                outer.set_exception(res)
                return
        elif fut._exception is not None:
            res = fut.exception()  # Mark exception retrieved.
            if not return_exceptions:
                outer.set_exception(res)
                return
        else:
            res = fut._result
        results[i] = res
        nfinished += 1
        if nfinished == nchildren:
            if outer._cancel_requested:
                outer.set_exception(futures.CancelledError())
            else:
                outer.set_result(results)

    for i, fut in enumerate(children):
        fut.add_done_callback(functools.partial(_done_callback, i))
    return outer


def sync_shield(arg, *, loop=None):
    """Wait for a future, shielding it from cancellation.
    The statement
        res = yield from shield(something())
    is exactly equivalent to the statement
        res = yield from something()
    *except* that if the coroutine containing it is cancelled, the
    task running in something() is not cancelled.  From the POV of
    something(), the cancellation did not happen.  But its caller is
    still cancelled, so the yield-from expression still raises
    CancelledError.  Note: If something() is cancelled by other means
    this will still cancel shield().
    If you want to completely ignore cancellation (not recommended)
    you can combine shield() with a try/except clause, as follows:
        try:
            res = yield from shield(something())
        except CancelledError:
            res = None
    """
    ### use ensure_sync_future instead of ensure_future ###
    inner = ensure_sync_future(arg, loop=loop)
    if inner.done():
        # Shortcut.
        return inner
    loop = inner._loop
    ### use SyncFuture instead of loop.create_future ###
    outer = SyncFuture(loop=loop)

    def _done_callback(inner):
        if outer.cancelled():
            if not inner.cancelled():
                # Mark inner's result as retrieved.
                inner.exception()
            return

        if inner.cancelled():
            outer.cancel()
        else:
            exc = inner.exception()
            if exc is not None:
                outer.set_exception(exc)
            else:
                outer.set_result(inner.result())

    inner.add_done_callback(_done_callback)
    return outer
