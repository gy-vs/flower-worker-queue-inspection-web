import asyncio
import threading
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import Mock

from kombu.exceptions import OperationalError

from flower.inspector import Inspector


# pylint: disable=protected-access
class InspectorTests(TestCase):
    def test_logs_broker_failure_without_propagating_it(self):
        capp = Mock()
        capp.control.inspect.return_value.registered.side_effect = (
            OperationalError('broker is down'))
        inspector = Inspector(Mock(), capp, timeout=1)

        with self.assertLogs('flower.inspector', level='WARNING') as logs:
            inspector._inspect('registered', None, 0)

        self.assertEqual(
            'WARNING:flower.inspector:'
            'Inspect method registered failed: broker is down',
            logs.output[0],
        )

    def test_handles_transport_specific_connection_error(self):
        class TransportConnectionError(Exception):
            pass

        capp = Mock()
        capp.control.inspect.return_value.active.side_effect = (
            TransportConnectionError('connection closed by server'))
        connection = capp.connection_for_read.return_value
        connection.recoverable_connection_errors = (
            TransportConnectionError,)
        inspector = Inspector(Mock(), capp, timeout=1)

        with self.assertLogs('flower.inspector', level='WARNING') as logs:
            inspector._inspect('active', None, 0)

        self.assertEqual(
            'WARNING:flower.inspector:'
            'Inspect method active failed: connection closed by server',
            logs.output[0],
        )
        connection.close.assert_called_once_with()


class InspectorCacheOrderingTests(IsolatedAsyncioTestCase):
    """Inspect RPCs can complete out of order; the cache must never go backwards."""

    QUEUES_OLD = {'worker1': [{'name': 'old'}]}
    QUEUES_BOTH = {'worker1': [{'name': 'old'}, {'name': 'new'}]}

    async def _drain_callbacks(self):
        # _on_update is scheduled from executor threads via add_callback
        for _ in range(20):
            await asyncio.sleep(0.001)

    async def _wait_event(self, event):
        for _ in range(2000):
            if event.is_set():
                return
            await asyncio.sleep(0.001)
        self.assertTrue(event.is_set())

    def _scripted_capp(self, global_started, worker_started):
        def make_inspect(timeout, destination):
            controller = Mock()

            def active_queues():
                event = worker_started if destination else global_started
                event.set()
                event.wait(timeout=2)
                return self.QUEUES_BOTH if destination else self.QUEUES_OLD

            controller.active_queues.side_effect = active_queues
            return controller

        capp = Mock()
        capp.control.inspect.side_effect = make_inspect
        return capp

    def test_on_update_ignores_stale_responses(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)

        inspector._on_update('worker1', 'active_queues',
                             self.QUEUES_BOTH['worker1'], 1)
        # an older RPC (e.g. an earlier global refresh) finishing later
        inspector._on_update('worker1', 'active_queues',
                             self.QUEUES_OLD['worker1'], 0)

        self.assertEqual(
            self.QUEUES_BOTH['worker1'],
            inspector.workers['worker1']['active_queues'],
        )

    def test_on_update_applies_responses_in_dispatch_order(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)

        inspector._on_update('worker1', 'active_queues',
                             self.QUEUES_OLD['worker1'], 0)
        inspector._on_update('worker1', 'active_queues',
                             self.QUEUES_BOTH['worker1'], 1)

        self.assertEqual(
            self.QUEUES_BOTH['worker1'],
            inspector.workers['worker1']['active_queues'],
        )

    def test_stale_response_for_one_worker_does_not_affect_others(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)

        inspector._on_update('worker1', 'active_queues',
                             self.QUEUES_BOTH['worker1'], 1)
        inspector._on_update('worker2', 'active_queues',
                             [{'name': 'other'}], 0)

        self.assertEqual(
            self.QUEUES_BOTH['worker1'],
            inspector.workers['worker1']['active_queues'],
        )
        self.assertEqual(
            [{'name': 'other'}],
            inspector.workers['worker2']['active_queues'],
        )

    async def test_overlapping_global_and_worker_inspect_keeps_newer_state(self):
        # Reproduces the controlled interleaving:
        # 1. global refresh is dispatched and blocks holding the old state
        # 2. single-worker refresh is dispatched, completes with old+new
        # 3. the earlier global refresh completes afterwards with stale old
        loop = asyncio.get_running_loop()
        global_started, release_global = threading.Event(), threading.Event()
        worker_started, release_worker = threading.Event(), threading.Event()

        io_loop = Mock()
        io_loop.run_in_executor.side_effect = loop.run_in_executor
        io_loop.add_callback.side_effect = loop.call_soon_threadsafe
        capp = self._scripted_capp(global_started, worker_started)
        inspector = Inspector(io_loop, capp, timeout=1, max_concurrency=4)
        inspector.inspect_methods = ('active_queues',)

        global_task = inspector.inspect()
        await self._wait_event(global_started)

        worker_task = inspector.inspect('worker1')
        await self._wait_event(worker_started)

        # single-worker inspection completes first with old+new
        release_worker.set()
        await worker_task
        await self._drain_callbacks()
        self.assertEqual(
            self.QUEUES_BOTH['worker1'],
            inspector.workers['worker1']['active_queues'],
        )

        # the earlier, stale global inspection finishes later
        release_global.set()
        await global_task
        await self._drain_callbacks()

        # cache must keep the fresher confirmed state, not fall back to old
        self.assertEqual(
            self.QUEUES_BOTH['worker1'],
            inspector.workers['worker1']['active_queues'],
        )


class InspectorConcurrencyTests(IsolatedAsyncioTestCase):
    async def test_coalesces_refreshes_for_the_same_worker(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)
        complete = asyncio.Event()

        async def inspect_all(_):
            await complete.wait()

        inspector._inspect_all = inspect_all

        first = inspector.inspect('worker1')
        second = inspector.inspect('worker1')

        self.assertIs(first, second)
        complete.set()
        await first

    async def test_global_refresh_does_not_satisfy_worker_refresh(self):
        # the global refresh skips the task lists a worker page needs
        inspector = Inspector(Mock(), Mock(), timeout=1)
        complete = asyncio.Event()

        async def inspect_all(_):
            await complete.wait()

        inspector._inspect_all = inspect_all

        all_workers = inspector.inspect()
        one_worker = inspector.inspect('worker1')

        self.assertIsNot(all_workers, one_worker)
        complete.set()
        await all_workers
        await one_worker

    async def test_global_refresh_skips_task_lists(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)
        called = []

        async def inspect_method(method, workername):
            called.append((method, workername))

        inspector._inspect_method = inspect_method
        await inspector.inspect()

        self.assertEqual(
            [('stats', None), ('active_queues', None), ('registered', None), ('conf', None)],
            called)

    async def test_worker_refresh_runs_every_method(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)
        called = []

        async def inspect_method(method, workername):
            called.append(method)

        inspector._inspect_method = inspect_method
        await inspector.inspect('worker1')

        self.assertEqual(list(Inspector.inspect_methods), called)

    async def test_bounds_inspector_concurrency(self):
        io_loop = Mock()
        pending = []

        def run_in_executor(*_):
            future = asyncio.get_running_loop().create_future()
            pending.append(future)
            return future

        io_loop.run_in_executor.side_effect = run_in_executor
        inspector = Inspector(io_loop, Mock(), timeout=1, max_concurrency=2)
        inspector.inspect_methods = ('stats', 'active', 'conf')

        operation = inspector.inspect('worker1')
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(2, len(pending))

        pending[0].set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(3, len(pending))

        pending[1].set_result(None)
        pending[2].set_result(None)
        await operation
