import abc
import asyncio
import enum


class TaskState(enum.Enum):
    INIT = enum.auto()
    UNREADY = enum.auto()
    WAIT = enum.auto()
    PROCESS = enum.auto()
    CLOSED = enum.auto()
    JOIN = enum.auto()
    COMMIT = enum.auto()
    REBALANCE = enum.auto()


class Partitionable(abc.ABC):

    def partitions(self):
        return None

    @abc.abstractmethod
    def partitionate(self, partition): pass


class PartitionArgument(abc.ABC):

    async def start(self):
        pass

    async def stop(self):
        pass

    async def before_commit(self):
        pass

    async def after_commit(self):
        pass


class PartitionTask:

    def __init__(self, app, fun, kargs, kwargs):
        self._app = app
        self._state = TaskState.INIT if app.ready else TaskState.UNREADY
        self._fun = fun
        self._kargs = kargs
        self._kwargs = kwargs
        self._partitionables = []
        for arg in itertools.chain(kargs, kwargs.values()):
            if isinstance(arg, Partitionable):
                self._partitionables.append(arg)
        self._pargs = None # the computed partition arguments
        self._task = None # the main task
        self._commit = None # the commit task

    async def before_rebalance(self, revoked):
        # commit if not already done
        await self.before_commit()
        # not started yet, nothing to do
        if self._state is TaskState.UNREADY:
            return
        # already rebalancing
        elif self._state is TaskState.REBALANCE:
            return
        # stop the task
        elif self._state is TaskState.COMMIT
            self._state = TaskState.REBALANCE
            task = self._task
            self._task = None
            if task and not task.done():
                task.cancel()
                await asyncio.wait([task])
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def after_rebalance(self, assigned):
        # not started yet, don't start anything
        if self._state in (TaskState.INIT, TaskState.UNREADY):
            self._state = TaskState.INIT
        # start the task over
        elif self._state in (TaskState.WAIT, TaskState.REBALANCE):
            self._state = TaskState.PROCESS
            self._task = asyncio.ensure_future(self._run())
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def before_commit(self):
        # already committing, nothing to do
        if self._state in (TaskState.COMMIT, TaskState.REBALANCE):
            return
        # not started yet, tag it unready to start
        elif self._state in (TaskState.INIT, TaskState.UNREADY):
            self._state = TaskState.UNREADY
            return
        # already joining, just wait for it to finish
        elif self._state is TaskState.JOIN:
            return await self._commit
        # currently processing, join all the partition arguments
        elif self._state is TaskState.PROCESS:
            # no partition argument, nothing to do
            if not self._pargs:
                self._state = TaskState.COMMIT
                return
            # join all the partition arguments
            self._state = TaskState.JOIN
            async def aux(pargs):
                tasks = [arg.before_commit() for arg in pargs]
                try:
                    await asyncio.gather(tasks)
                    self._state = TaskState.COMMIT
                    self._commit = None
                # joining went wrong, stop everything
                # and tag waiting to restart
                except:
                    self._commit = None
                    self._state = TaskState.WAIT
                    if self._task and not self._task.done():
                        tasks.append(self._task)
                    self._task = None
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    if self._task and not self._task.done():
                        self._task.cancel()
                        tasks.append(self._task)
                    await asyncio.wait(tasks)
                    raise
            # save this task for other calls
            self._commit = async.ensure_future(aux(self._pargs))
            return await self._commit
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def after_commit(self):
        # not started yet, don't start anything
        if self._state in (TaskState.INIT, TaskState.UNREADY):
            self._state = TaskState.INIT
        # waiting for commit to start, start it noe
        elif self._state is TaskState.WAIT:
            self._state = TaskState.PROCESS
            self._task = asyncio.ensure_future(self._run())
        #  comitting, notify all the partition arguments
        elif self._state is TaskState.COMMIT
            if self._pargs:
                self._state = TaskState.PROCESS
                await self.gather([arg.after_commit() for arg in self._pargs])
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def start(self):
        self._app.register_task(self)
        # ready to start
        if self._state is TaskState.INIT:
            self._task = asyncio.ensure_future(self._run())
            self._state = TaskState.PROCESS
        # in the middle of a commit or a rebalance, start after it finished
        elif self._state is TaskState.UNREADY:
            self._state = TaskState.WAIT
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def stop(self):
        # already closed or not even started
        if self._state in (
                TaskState.INIT, TaskState.UNREADY, TaskState.CLOSED):
            self._state = TaskState.CLOSED
            return

        try: self._app.unregister_task(self)
        except: log.error(
                'Exception when unregistering the task', exc_info=True)
        # one last commit
        try: await self.before_commit()
        except: log.error(
                'Exception when committing the task', exc_info=True)
        # check that the state makes sense
        if self._state not in (
                TaskState.WAIT, TaskState.COMMIT, TaskState.REBALANCE):
            log.error('Unexpected state %s on closing', self._state.name)
        self._state = TaskState.CLOSED
        # cancel the task
        if self._task and not self._task.done():
            await asyncio.wait([await self._task])

    async def _run(self):
        """The main coroutine"""
        # get the partition of all the partitionables
        # and check that there are the same
        partitions = None
        for arg in self._partitionables:
            p = arg.partitions()
            if p is None: pass
            elif partitions is None:
                partitions = p
            else:
                assert partitions == p
        assert partitions is not None
        if not partitions: return
        # build the arguments
        args = {}
        pargs = []
        for partition in partitions:
            def transform(arg):
                if isinstance(arg, Partitionable):
                    arg = arg.partitionate(partition)
                    if insinstance(arg):
                        pargs.append(arg, PartitionArgument)
                return arg
            kargs = [transform(arg) for arg in self._kargs]
            kwargs = {k: transform(arg) for k, arg in self._kwargs.items()}
            args.append((kargs, kwargs))
        # start the partition arguments and then the tasks
        self._pargs = pargs
        tasks = ()
        try:
            if pargs:
                tasks = [arg.start() for arg in pargs]
                await self.gather(tasks)

            # TODO : stop  pargs of a task once finished
            tasks = [asyncio.ensure_future(self._fun(*kargs, **kwargs))
                    for kargs, kwargs in args.values()]
            await self.gather(tasks)
        # cancel everything and close the partition arguments
        except asyncio.CancelledError:
            raise
        except:
            log.error('Exception while running the tasks', exc_info=True)
            raise
        finally:
            self._pargs = None
            if tasks:
                for task in tasks:
                    if not task.done(): task.cancel()
                await self.gather(tasks, return_exception=True)
            if pargs:
                await self.gather([arg.stop() for arg in pargs],
                        return_exception=True)
