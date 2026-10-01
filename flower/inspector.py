import asyncio
import collections
import itertools
import logging
import time
from functools import partial

from kombu.exceptions import OperationalError

logger = logging.getLogger(__name__)


class Inspector:
    inspect_methods = ('stats', 'active_queues', 'registered', 'scheduled',
               'active', 'reserved', 'revoked', 'conf')
    # Task lists can be huge, they are fetched only when one worker is inspected
    task_inspect_methods = ('scheduled', 'active', 'reserved', 'revoked')
    max_concurrency = len(inspect_methods)

    def __init__(self, io_loop, capp, timeout, max_concurrency=None):
        self.io_loop = io_loop
        self.capp = capp
        self.timeout = timeout
        self.workers = collections.defaultdict(dict)
        self._inspect_tasks = {}
        self._inspect_max_concurrency = (
            max_concurrency or self.max_concurrency)
        self._inspect_semaphore = None
        # Generation of the latest inspect RPC applied per (worker, method).
        # Inspect RPCs run in a thread pool and can complete out of order
        # (e.g. a global refresh finishing after a single-worker refresh);
        # stale responses must never overwrite fresher cached state.
        self._inspect_generation = itertools.count()
        self._latest_update = collections.defaultdict(dict)

    def inspect(self, workername=None):
        task = self._inspect_tasks.get(workername)
        if task is None:
            task = asyncio.ensure_future(self._inspect_all(workername))
            self._inspect_tasks[workername] = task
            task.add_done_callback(
                partial(self._on_inspect_done, workername))
        return task

    def methods_to_inspect(self, workername):
        if workername is not None:
            return self.inspect_methods
        return tuple(method for method in self.inspect_methods if method not in self.task_inspect_methods)

    async def _inspect_all(self, workername):
        methods = self.methods_to_inspect(workername)
        results = await asyncio.gather(*(
            self._inspect_method(method, workername)
            for method in methods
        ), return_exceptions=True)
        for method, result in zip(methods, results):
            if isinstance(result, Exception):
                logger.error("Inspect method %s failed: %s", method, result)

    async def _inspect_method(self, method, workername):
        if self._inspect_semaphore is None:
            self._inspect_semaphore = asyncio.Semaphore(
                self._inspect_max_concurrency)
        async with self._inspect_semaphore:
            # Stamp on dispatch in the event loop thread; a response is only
            # applied while its generation stays the latest one observed.
            generation = next(self._inspect_generation)
            await self.io_loop.run_in_executor(
                None, partial(self._inspect, method, workername, generation))

    def _on_inspect_done(self, workername, task):
        if self._inspect_tasks.get(workername) is task:
            self._inspect_tasks.pop(workername)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Worker inspection failed: %s", task.exception())

    def _on_update(self, workername, method, response, generation):
        latest = self._latest_update[workername].get(method, -1)
        if generation < latest:
            logger.debug(
                "Discarding stale %s inspect response for '%s' "
                "(generation %s, latest %s)",
                method, workername, generation, latest)
            return
        self._latest_update[workername][method] = generation

        if method == 'stats':
            consumer = response.get('consumer') or response
            broker = consumer.get('broker', {})
            # Temporary fix for issue #1512, until Celery sanitizes broker statistics.
            broker.pop('alternates', None)

        info = self.workers[workername]
        info[method] = response
        info['timestamp'] = time.time()

    def _inspect(self, method, workername, generation):
        destination = [workername] if workername else None
        inspect = self.capp.control.inspect(timeout=self.timeout, destination=destination)

        logger.debug('Sending %s inspect command', method)
        start = time.time()
        try:
            result = (
                getattr(inspect, method)()
                if method != 'active'
                else getattr(inspect, method)(safe=True)
            )
        except Exception as exc:
            if not self._is_connection_error(exc):
                raise
            logger.warning("Inspect method %s failed: %s", method, exc)
            return
        logger.debug("Inspect command %s took %.2fs to complete", method, time.time() - start)

        if result is None or 'error' in result:
            logger.warning("Inspect method %s failed", method)
            return
        for worker, response in result.items():
            if response is not None:
                self.io_loop.add_callback(
                    partial(self._on_update, worker, method, response,
                            generation))

    def _is_connection_error(self, exc):
        if isinstance(exc, OperationalError):
            return True

        connection = self.capp.connection_for_read()
        try:
            return isinstance(exc, connection.recoverable_connection_errors)
        finally:
            connection.close()
