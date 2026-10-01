import asyncio
from functools import partial
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
            inspector._inspect('registered', None, 1)

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
            inspector._inspect('active', None, 1)

        self.assertEqual(
            'WARNING:flower.inspector:'
            'Inspect method active failed: connection closed by server',
            logs.output[0],
        )
        connection.close.assert_called_once_with()


class InspectorStaleResponseTests(IsolatedAsyncioTestCase):
    """Responses can come back in a different order than the commands were sent.

    Scenario from the queue add flow: a global refresh (started first) is still
    holding the old result when a per-worker refresh reads the new state, and
    the global response lands only afterwards.  The late global response must
    not roll the worker's cached active queues back to the stale value.
    """

    async def test_late_global_response_does_not_roll_back_newer_worker_state(self):
        io_loop = Mock()
        pending = []

        # hand control of the blocking inspect call back to the test: capture
        # its version and keep its future pending until the test decides that
        # the broker response has arrived
        def run_in_executor(_executor, func):
            version = func.args[-1]
            future = asyncio.get_running_loop().create_future()
            pending.append((version, future))
            return future

        io_loop.run_in_executor.side_effect = run_in_executor
        io_loop.add_callback.side_effect = lambda callback: callback()

        inspector = Inspector(io_loop, Mock(), timeout=1)
        inspector.inspect_methods = ('active_queues',)

        def deliver(version, queues):
            # mimic the executor thread finishing _inspect, which schedules the
            # cache update back onto the I/O loop
            io_loop.add_callback(partial(
                inspector._on_update, 'worker1', 'active_queues',
                queues, version))

        async def wait_pending():
            for _ in range(100):
                if pending:
                    return
                await asyncio.sleep(0)
            self.fail("inspect command was not issued")

        async def wait_queues(expected):
            for _ in range(100):
                if inspector.workers.get('worker1', {}).get('active_queues') == expected:
                    return
                await asyncio.sleep(0)
            self.fail(f"queues never reached {expected!r}")

        # global refresh starts first and is held with the old result
        global_refresh = inspector.inspect()
        await wait_pending()
        global_version = pending.pop(0)[0]

        # add_consumer is confirmed in between; the per-worker refresh then
        # reads old + new and lands in the cache first
        worker_refresh = inspector.inspect('worker1')
        await wait_pending()
        worker_version = pending.pop(0)[0]
        old_and_new = [{'name': 'old'}, {'name': 'new'}]
        deliver(worker_version, old_and_new)
        await wait_queues(old_and_new)

        # only now does the earlier global refresh come back with its old view
        deliver(global_version, [{'name': 'old'}])
        for _ in range(100):
            await asyncio.sleep(0)

        self.assertEqual(
            old_and_new,
            inspector.workers['worker1']['active_queues'],
            "a late global response must not overwrite a fresher worker result")
        worker_refresh.cancel()
        global_refresh.cancel()

    async def test_equal_or_newer_versions_are_applied(self):
        inspector = Inspector(Mock(), Mock(), timeout=1)

        inspector._on_update('worker1', 'active_queues',
                             [{'name': 'old'}], 1)
        inspector._on_update('worker1', 'active_queues',
                             [{'name': 'old'}, {'name': 'new'}], 2)
        inspector._on_update('worker1', 'active_queues',
                             [{'name': 'old'}], 1)

        self.assertEqual(
            ['old', 'new'],
            [q['name'] for q in
             inspector.workers['worker1']['active_queues']])
        self.assertEqual(
            2, inspector._applied_versions[('worker1', 'active_queues')])


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
