import abc
import asyncio
import enum
import itertools
import logging

log = logging.getLogger(__name__)


class TaskState(enum.Enum):
    INIT = enum.auto()
    WAIT = enum.auto()
    PROCESS = enum.auto()
    CLOSED = enum.auto()
    JOIN = enum.auto()
    COMMIT = enum.auto()
    REBALANCE = enum.auto()


class AbstractTask(abc.ABC):

    async def start(self):
        pass

    async def stop(self):
        pass

    async def before_rebalance(self, revoked):
        pass

    async def after_rebalance(self, assigned):
        pass

    async def before_commit(self):
        pass

    async def after_commit(self):
        pass


class Partitionable(abc.ABC):
    """
    The ABC of all the arguments of `PartitionTask` that can be partitioned
    """

    @property
    def topics(self):
        """
        Returns the set of
        """
        return ()

    def partitions(self):
        """
        Returns the set partitions available, or `None` if it doesn't matter
        """
        return None

    @abc.abstractmethod
    def partitionate(self, partition):
        """
        Returns the argument for each partition.
        Can be an instance of `PartitionArgument`
        """
        pass


class _GetPartition(Partitionable):
    """
    A simple partitionable which just returns the selected partition
    """

    def partitionate(self, partition):
        return partition

get_parition = _GetPartition()


class PartitionArgument(abc.ABC):
    """
    The ABC of the `Partitionable.partitionate` results
    which should be notified by commits
    """

    async def start(self):
        """
        Called when the task starts
        """
        pass

    async def stop(self):
        """
        Called when the tasks stops
        """
        pass

    async def before_commit(self):
        """
        Called before starting the commit
        Must wait until the commit is ready or raise if impossible
        """
        pass

    async def during_commit(self):
        """
        Called before starting the commit
        Must wait until the commit is ready or raise if impossible
        """
        pass

    async def after_commit(self):
        """
        Called when the commit has happened and the task can be resumed
        """
        pass


class PartitionTask(AbstractTask):
    """
    A task that will run a function for each partition available
    """

    def __init__(self, app, fun, kargs, kwargs):
        self._app = app
        self._state = TaskState.INIT
        self._fun = fun
        self._kargs = kargs
        self._kwargs = kwargs
        self._partitionables = []
        group = set()
        for arg in itertools.chain(kargs, kwargs.values()):
            if isinstance(arg, Partitionable):
                self._partitionables.append(arg)
                topics = arg.topics
                if topics: group.update(topics)
        self._pargs = None # the computed partition arguments
        self._task = None # the main task
        self._commit = None # the commit task
        if len(group) > 1:
            app.group(*group)

    async def before_rebalance(self, revoked):
        # commit if not already done
        print ('before_rebalance a')
        await self.before_commit()
        print ('before_rebalance b')
        # already rebalancing
        if self._state is TaskState.REBALANCE:
            return
        # stop the task
        elif self._state is TaskState.COMMIT:
            self._state = TaskState.REBALANCE
            task = self._task
            self._task = None
            if task and not task.done():
                task.cancel()
                print ('before_rebalance c')
                task.print_stack()
                await asyncio.wait([task])
        else: raise RuntimeError('state ??? ' + self._state.name)
        print ('before_rebalance d')

    async def after_rebalance(self, assigned):
        # not started yet, don't start anything
        if self._state is TaskState.INIT:
            return
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
        elif self._state is TaskState.INIT:
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
                if not pargs:
                    return
                tasks = ()
                try:
                    tasks = [asyncio.ensure_future(arg.before_commit())
                            for arg in pargs]
                    await asyncio.gather(*tasks)
                    tasks = [asyncio.ensure_future(arg.during_commit())
                            for arg in pargs]
                    await asyncio.gather(*tasks)
                    self._state = TaskState.COMMIT
                    self._commit = None
                # joining went wrong, stop everything
                # and tag waiting to restart
                except Exception as e:
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
            self._commit = asyncio.ensure_future(aux(self._pargs))
            return await self._commit
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def after_commit(self):
        # not started yet, don't start anything
        if self._state is TaskState.INIT:
            return
        # waiting for commit to start, start it noe
        elif self._state is TaskState.WAIT:
            self._state = TaskState.PROCESS
            self._task = asyncio.ensure_future(self._run())
        #  comitting, notify all the partition arguments
        elif self._state is TaskState.COMMIT:
            if self._pargs:
                self._state = TaskState.PROCESS
                await asyncio.gather(*(arg.after_commit() for arg in self._pargs))
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def start(self):
        self._app.register_task(self)
        # ready to start
        if self._state is TaskState.INIT:
            if self._app.ready:
                self._task = asyncio.ensure_future(self._run())
                self._state = TaskState.PROCESS
            else:
                self._state = TaskState.WAIT
        # in the middle of a commit or a rebalance, start after it finished
        else: raise RuntimeError('state ??? ' + self._state.name)

    async def stop(self):
        # already closed or not even started
        if self._state in (TaskState.INIT, TaskState.CLOSED):
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
            self._task.cancel()
            await asyncio.wait([self._task])

    async def _run(self):
        """The main coroutine"""
        # get the partition of all the partitionables
        # and check that there are the same
        partitions = None
        for arg in self._partitionables:
            p = arg.partitions()
            if p is None: pass
            elif partitions is None:
                partitions = set(p)
            else:
                assert partitions == set(p)
        assert partitions is not None
        if not partitions: return
        # build the arguments
        args = {}
        pargs = []
        for partition in partitions:
            def transform(arg):
                if isinstance(arg, Partitionable):
                    arg = arg.partitionate(partition)
                    if isinstance(arg, PartitionArgument):
                        pargs.append(arg)
                return arg
            kargs = [transform(arg) for arg in self._kargs]
            kwargs = {k: transform(arg) for k, arg in self._kwargs.items()}
            args[partition] = (kargs, kwargs)
        # start the partition arguments and then the tasks
        self._pargs = pargs
        tasks = ()
        try:
            if pargs:
                tasks = [asyncio.ensure_future(arg.start()) for arg in pargs]
                print ('_run 1')
                await asyncio.gather(*tasks)

            # TODO : stop  pargs of a task once finished
            tasks = [asyncio.ensure_future(self._fun(*kargs, **kwargs))
                    for kargs, kwargs in args.values()]
            print ('_run 2', tasks)
            await asyncio.gather(*tasks)
        # cancel everything and close the partition arguments
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error('Exception while running the tasks', exc_info=True)
            raise
        finally:
            print ('_run 3')
            self._pargs = None
            if tasks:
                for task in tasks:
                    if not task.done(): task.cancel()
                print ('_run 4')
                await asyncio.wait(tasks)
            if pargs:
                print ('_run 5')
                await asyncio.wait([arg.stop() for arg in pargs])
            print ('_run 6')
