import asyncio
import json
import os
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

from kombu.exceptions import OperationalError
from tornado.httpclient import HTTPRequest
from tornado.options import options
from tornado.testing import gen_test

from flower.api.control import ControlHandler
from flower.inspector import Inspector

from . import BaseApiTestCase


class UnknownWorkerControlTests(BaseApiTestCase):
    def test_unknown_worker(self):
        r = self.post('/api/worker/shutdown/test', body={})
        self.assertEqual(404, r.code)

    def test_unknown_worker_error_is_not_html(self):
        r = self.post(
            '/api/worker/shutdown/%3Cimg%20src%3Dx%20onerror%3Dalert(1)%3E',
            body={})
        self.assertEqual(404, r.code)
        self.assertTrue(r.headers['Content-Type'].startswith('text/plain'))
        self.assertIn(b'<img src=x onerror=alert(1)>', r.body)


class WorkerControlTests(BaseApiTestCase):
    def setUp(self):
        super().setUp()
        is_worker = patch.object(ControlHandler, 'is_worker', return_value=True)
        self.addCleanup(is_worker.stop)
        is_worker.start()

    def test_shutdown(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock()
        r = self.post('/api/worker/shutdown/test', body={})
        self.assertEqual(200, r.code)
        celery.control.broadcast.assert_called_once_with('shutdown',
                                                         destination=['test'])

    def test_shutdown_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.broadcast = MagicMock()
            r = self.post('/api/worker/shutdown/test', body={})
            self.assertEqual(403, r.code)
            celery.control.broadcast.assert_not_called()
    @gen_test
    async def test_blocked_control_operation_does_not_block_healthcheck(self):
        celery = self._app.capp
        started = threading.Event()
        release = threading.Event()

        def block(*_args, **_kwargs):
            started.set()
            release.wait(timeout=2)

        celery.control.broadcast = MagicMock(side_effect=block)
        shutdown = self.http_client.fetch(HTTPRequest(
            self.get_url('/api/worker/shutdown/test'),
            method='POST',
            body='',
        ))
        try:
            for _ in range(100):
                if started.is_set():
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(started.is_set())

            started_at = time.monotonic()
            healthcheck = await self.http_client.fetch(
                self.get_url('/healthcheck'))
            elapsed = time.monotonic() - started_at
            self.assertEqual(200, healthcheck.code)
            # the control call is still blocked, the healthcheck must not have waited for it
            self.assertFalse(shutdown.done())
            self.assertLess(elapsed, 1.0)
        finally:
            release.set()

        response = await shutdown
        self.assertEqual(200, response.code)

    def test_broker_connection_failure_returns_service_unavailable(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            side_effect=OperationalError('broker is down'))

        r = self.post('/api/worker/shutdown/test', body={})

        self.assertEqual(503, r.code)
        self.assertEqual(b'Broker or result backend unavailable', r.body)

    def test_unexpected_failure_returns_internal_server_error(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            side_effect=ValueError('unexpected'))

        r = self.post('/api/worker/shutdown/test', body={})

        self.assertEqual(500, r.code)

    def test_pool_restart(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(return_value=[{'test': 'ok'}])
        r = self.post('/api/worker/pool/restart/test', body={})
        self.assertEqual(200, r.code)
        celery.control.broadcast.assert_called_once_with(
            'pool_restart',
            arguments={'reload': False},
            destination=['test'],
            reply=True,
        )

    def test_pool_restart_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.broadcast = MagicMock()
            r = self.post('/api/worker/pool/restart/test', body={})
            self.assertEqual(403, r.code)
            celery.control.broadcast.assert_not_called()

    def test_pool_grow(self):
        celery = self._app.capp
        celery.control.pool_grow = MagicMock(return_value=[{'test': 'ok'}])
        r = self.post('/api/worker/pool/grow/test', body={'n': 3})
        self.assertEqual(200, r.code)
        celery.control.pool_grow.assert_called_once_with(
            n=3, reply=True, destination=['test'])

    def test_pool_grow_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.pool_grow = MagicMock()
            r = self.post('/api/worker/pool/grow/test', body={'n': 3})
            self.assertEqual(403, r.code)
            celery.control.pool_grow.assert_not_called()

    def test_pool_shrink(self):
        celery = self._app.capp
        celery.control.pool_shrink = MagicMock(return_value=[{'test': 'ok'}])
        r = self.post('/api/worker/pool/shrink/test', body={})
        self.assertEqual(200, r.code)
        celery.control.pool_shrink.assert_called_once_with(
            n=1, reply=True, destination=['test'])

    def test_pool_shrink_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.pool_shrink = MagicMock()
            r = self.post('/api/worker/pool/shrink/test', body={})
            self.assertEqual(403, r.code)
            celery.control.pool_shrink.assert_not_called()

    def test_pool_autoscale(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(return_value=[{'test': 'ok'}])
        r = self.post('/api/worker/pool/autoscale/test',
                      body={'min': 2, 'max': 5})
        self.assertEqual(200, r.code)
        celery.control.broadcast.assert_called_once_with(
            'autoscale',
            reply=True, destination=['test'],
            arguments={'min': 2, 'max': 5})

    def test_pool_autoscale_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.broadcast = MagicMock()
            r = self.post('/api/worker/pool/autoscale/test',
                          body={'min': 2, 'max': 5})
            self.assertEqual(403, r.code)
            celery.control.broadcast.assert_not_called()

    def test_add_consumer(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value={'foo'})):
            r = self.post('/api/worker/queue/add-consumer/test',
                          body={'queue': 'foo'})
        self.assertEqual(200, r.code)
        celery.control.broadcast.assert_called_once_with(
            'add_consumer',
            reply=True, destination=['test'],
            arguments={'queue': 'foo'})

    def test_add_consumer_not_consumed_returns_conflict(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        # worker acknowledged the command but still consumes only 'old'
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value={'old'})):
            r = self.post('/api/worker/queue/add-consumer/test',
                          body={'queue': 'foo'})
        self.assertEqual(503, r.code)
        self.assertIn('still not consuming', r.body.decode('utf-8'))

    def test_add_consumer_unverifiable_state_returns_conflict(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        # the control was confirmed but no trustworthy queue list came back
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value=None)):
            r = self.post('/api/worker/queue/add-consumer/test',
                          body={'queue': 'foo'})
        self.assertEqual(503, r.code)
        self.assertIn('could not be verified', r.body.decode('utf-8'))

    def test_add_consumer_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.broadcast = MagicMock()
            r = self.post('/api/worker/queue/add-consumer/test',
                          body={'queue': 'foo'})
            self.assertEqual(403, r.code)
            celery.control.broadcast.assert_not_called()

    def test_add_consumer_missing_queue(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock()
        r = self.post('/api/worker/queue/add-consumer/test', body={})
        self.assertEqual(400, r.code)
        self.assertIn('Missing argument queue', r.body.decode('utf-8'))
        celery.control.broadcast.assert_not_called()

    def test_cancel_consumer_missing_queue(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock()
        r = self.post('/api/worker/queue/cancel-consumer/test', body={})
        self.assertEqual(400, r.code)
        celery.control.broadcast.assert_not_called()

    def test_cancel_consumer(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value={'old'})):
            r = self.post('/api/worker/queue/cancel-consumer/test',
                          body={'queue': 'foo'})
        self.assertEqual(200, r.code)
        celery.control.broadcast.assert_called_once_with(
            'cancel_consumer',
            reply=True, destination=['test'],
            arguments={'queue': 'foo'})

    def test_cancel_consumer_still_consumed_returns_conflict(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        # worker acknowledged the command but 'foo' is still in its queues
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value={'old', 'foo'})):
            r = self.post('/api/worker/queue/cancel-consumer/test',
                          body={'queue': 'foo'})
        self.assertEqual(503, r.code)
        self.assertIn('still consuming', r.body.decode('utf-8'))

    def test_cancel_consumer_unverifiable_state_returns_conflict(self):
        celery = self._app.capp
        celery.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value=None)):
            r = self.post('/api/worker/queue/cancel-consumer/test',
                          body={'queue': 'foo'})
        self.assertEqual(503, r.code)
        self.assertIn('could not be verified', r.body.decode('utf-8'))

    def test_cancel_consumer_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.broadcast = MagicMock()
            r = self.post('/api/worker/queue/cancel-consumer/test',
                          body={'queue': 'foo'})
            self.assertEqual(403, r.code)
            celery.control.broadcast.assert_not_called()

    def test_task_timeout(self):
        celery = self._app.capp
        celery.control.time_limit = MagicMock(
            return_value=[{'foo': {'ok': ''}}])

        r = self.post(
            '/api/task/timeout/celery.map',
            body={'workername': 'foo', 'hard': 3.1, 'soft': 1.2}
        )
        self.assertEqual(200, r.code)
        celery.control.time_limit.assert_called_once_with(
            'celery.map', hard=3.1, soft=1.2, destination=['foo'],
            reply=True)

    def test_task_timeout_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.time_limit = MagicMock()
            r = self.post('/api/task/timeout/celery.map',
                          body={'workername': 'foo', 'hard': 3.1, 'soft': 1.2})
            self.assertEqual(403, r.code)
            celery.control.time_limit.assert_not_called()

    def test_task_timeout_failure_returns_worker_error_message(self):
        celery = self._app.capp
        celery.control.time_limit = MagicMock(
            return_value=[{'foo': {'error': 'time limits not supported'}}])

        r = self.post(
            '/api/task/timeout/celery.map',
            body={'workername': 'foo', 'hard': 3.1, 'soft': 1.2}
        )
        self.assertEqual(403, r.code)
        self.assertEqual(b"Failed to set timeouts: 'time limits not supported'", r.body)

    def test_task_timeout_missing_workername(self):
        celery = self._app.capp
        celery.control.time_limit = MagicMock()

        r = self.post('/api/task/timeout/celery.map', body={'soft': 1.2})
        self.assertEqual(400, r.code)
        self.assertIn('Missing argument workername', r.body.decode('utf-8'))
        celery.control.time_limit.assert_not_called()

    def test_task_ratelimit_missing_workername(self):
        celery = self._app.capp
        celery.control.rate_limit = MagicMock()

        r = self.post('/api/task/rate-limit/celery.map', body={'ratelimit': 20})
        self.assertEqual(400, r.code)
        celery.control.rate_limit.assert_not_called()

    def test_task_ratelimit(self):
        celery = self._app.capp
        celery.control.rate_limit = MagicMock(
            return_value=[{'foo': {'ok': ''}}])

        r = self.post('/api/task/rate-limit/celery.map',
                      body={'workername': 'foo', 'ratelimit': 20})
        self.assertEqual(200, r.code)
        celery.control.rate_limit.assert_called_once_with(
            'celery.map', '20', destination=['foo'], reply=True)

    def test_task_ratelimit_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.rate_limit = MagicMock()
            r = self.post('/api/task/rate-limit/celery.map',
                          body={'workername': 'foo', 'ratelimit': 20})
            self.assertEqual(403, r.code)
            celery.control.rate_limit.assert_not_called()

    def test_task_ratelimit_non_integer(self):
        celery = self._app.capp
        celery.control.rate_limit = MagicMock(
            return_value=[{'foo': {'ok': ''}}])

        r = self.post('/api/task/rate-limit/celery.map',
                      body={'workername': 'foo', 'ratelimit': '11/m'})
        self.assertEqual(200, r.code)
        celery.control.rate_limit.assert_called_once_with(
            'celery.map', '11/m', destination=['foo'], reply=True)

    def test_task_ratelimit_failure_returns_worker_error_message(self):
        celery = self._app.capp
        celery.control.rate_limit = MagicMock(
            return_value=[{'foo': {'error': 'Invalid rate limit string'}}])

        r = self.post('/api/task/rate-limit/celery.map',
                      body={'workername': 'foo', 'ratelimit': 'garbage'})
        self.assertEqual(403, r.code)
        self.assertEqual(b"Failed to set rate limit: 'Invalid rate limit string'", r.body)

    def test_param_escape(self):
        app = self._app.capp
        app.control.broadcast = MagicMock(
            return_value=[{'test': {'ok': ''}}])
        with patch.object(ControlHandler, 'refresh_active_queues',
                          new=AsyncMock(return_value={'foo&amp;bar'})):
            r = self.post('/api/worker/queue/add-consumer/test',
                          body={'queue': 'foo&bar'})
        self.assertEqual(200, r.code)
        app.control.broadcast.assert_called_once_with(
            'add_consumer',
            reply=True, destination=['test'],
            arguments={'queue': 'foo&amp;bar'})

    def test_add_consumer_success_keeps_verified_queues_in_cache(self):
        # end to end through the real inspector and thread executor with a
        # deterministic control backend: a global refresh first caches 'old',
        # the worker acknowledges add_consumer 'new', and the verification
        # refresh must read old+new and leave that list in the cache that the
        # next page render reads
        celery = self._app.capp
        old = {'worker1': [{'name': 'old'}]}
        new = {'worker1': [{'name': 'old'}, {'name': 'new'}]}

        inspector_mock = MagicMock()
        inspector_mock.return_value.active_queues.side_effect = [
            old,   # the earlier global refresh
            new,   # verification after add_consumer was confirmed
        ]
        celery.control.inspect = inspector_mock
        celery.control.broadcast = MagicMock(
            return_value=[{'worker1': {'ok': 'add consumer new'}}])

        with patch.object(Inspector, 'inspect_methods', ('active_queues',)):
            # seed the cache the way the auto-refreshing monitor page does
            r = self.get('/api/workers?refresh=1')
            self.assertEqual(200, r.code)
            self.assertEqual(
                ['old'],
                [q['name'] for q in
                 self._app.workers['worker1']['active_queues']])

            r = self.post('/api/worker/queue/add-consumer/worker1',
                          body={'queue': 'new'})

        self.assertEqual(200, r.code)
        self.assertEqual(b'{"message": "add consumer new"}', r.body)
        self.assertEqual(
            ['old', 'new'],
            [q['name'] for q in
             self._app.workers['worker1']['active_queues']])
        # a later read of the cached worker info stays consistent
        r = self.get('/api/workers?workername=worker1')
        body = json.loads(r.body.decode('utf-8'))
        self.assertEqual(
            ['old', 'new'],
            [q['name'] for q in body['worker1']['active_queues']])

    def test_add_consumer_verification_failure_does_not_report_success(self):
        # control succeeded but the worker still reports the old queues: the
        # page must not show a success toast over a stale list
        celery = self._app.capp
        queues = {'worker1': [{'name': 'old'}]}
        inspector_mock = MagicMock()
        inspector_mock.return_value.active_queues.return_value = queues
        celery.control.inspect = inspector_mock
        celery.control.broadcast = MagicMock(
            return_value=[{'worker1': {'ok': 'add consumer new'}}])

        with patch.object(Inspector, 'inspect_methods', ('active_queues',)):
            self.get('/api/workers?refresh=1')
            r = self.post('/api/worker/queue/add-consumer/worker1',
                          body={'queue': 'new'})

        self.assertEqual(503, r.code)
        self.assertIn('still not consuming', r.body.decode('utf-8'))
        self.assertEqual(
            ['old'],
            [q['name'] for q in
             self._app.workers['worker1']['active_queues']])

    def test_cancel_consumer_success_keeps_verified_queues_in_cache(self):
        # symmetric chain for cancel-consumer: old+new cached, the worker
        # acknowledges cancel_consumer and the verification reads only old
        celery = self._app.capp
        with_new = {'worker1': [{'name': 'old'}, {'name': 'new'}]}
        without_new = {'worker1': [{'name': 'old'}]}

        inspector_mock = MagicMock()
        inspector_mock.return_value.active_queues.side_effect = [
            with_new,      # the earlier global refresh
            without_new,   # verification after cancel_consumer was confirmed
        ]
        celery.control.inspect = inspector_mock
        celery.control.broadcast = MagicMock(
            return_value=[{'worker1': {'ok': 'no longer consuming from new'}}])

        with patch.object(Inspector, 'inspect_methods', ('active_queues',)):
            r = self.get('/api/workers?refresh=1')
            self.assertEqual(200, r.code)
            self.assertEqual(
                ['old', 'new'],
                [q['name'] for q in
                 self._app.workers['worker1']['active_queues']])

            r = self.post('/api/worker/queue/cancel-consumer/worker1',
                          body={'queue': 'new'})

        self.assertEqual(200, r.code)
        self.assertEqual(
            b'{"message": "no longer consuming from new"}', r.body)
        self.assertEqual(
            ['old'],
            [q['name'] for q in
             self._app.workers['worker1']['active_queues']])

    @gen_test
    async def test_late_global_refresh_does_not_revert_verified_queues(self):
        # exact interleaving from the bug report:
        # 1) a global refresh is in flight and holds the old queue list
        # 2) add_consumer is confirmed; the per-worker verification reads
        #    old+new first and answers the POST successfully
        # 3) the stale global response lands last and must be discarded
        celery = self._app.capp
        global_started = threading.Event()
        release_global = threading.Event()

        def active_queues(destination):
            if destination is None:
                global_started.set()
                release_global.wait(timeout=5)
                return {'worker1': [{'name': 'old'}]}
            return {'worker1': [{'name': 'old'}, {'name': 'new'}]}

        def inspect_factory(timeout=None, destination=None):
            inspector_mock = MagicMock()
            inspector_mock.active_queues.side_effect = (
                lambda *args, **kwargs: active_queues(destination))
            return inspector_mock

        celery.control.inspect = MagicMock(side_effect=inspect_factory)
        celery.control.broadcast = MagicMock(
            return_value=[{'worker1': {'ok': 'add consumer new'}}])

        async def wait_for(event):
            for _ in range(300):
                if event.is_set():
                    return
                await asyncio.sleep(0.01)
            self.fail("event was never set")

        with patch.object(Inspector, 'inspect_methods', ('active_queues',)):
            global_refresh = self.http_client.fetch(
                self.get_url('/api/workers?refresh=1'))
            await wait_for(global_started)

            response = await self.http_client.fetch(HTTPRequest(
                self.get_url('/api/worker/queue/add-consumer/worker1'),
                method='POST', body='queue=new'))
            self.assertEqual(200, response.code)
            self.assertEqual(
                ['old', 'new'],
                [q['name'] for q in
                 self._app.workers['worker1']['active_queues']])

            release_global.set()
            self.assertEqual(200, (await global_refresh).code)
            # let the global response's cache-update callbacks drain
            await asyncio.sleep(0.1)

            # the late global response must not have rolled the cache back
            self.assertEqual(
                ['old', 'new'],
                [q['name'] for q in
                 self._app.workers['worker1']['active_queues']])
            body = await self.http_client.fetch(
                self.get_url('/api/workers?workername=worker1'))
            self.assertEqual(
                ['old', 'new'],
                [q['name'] for q in json.loads(body.body.decode('utf-8'))
                 ['worker1']['active_queues']])


class TaskControlTests(BaseApiTestCase):
    def test_revoke(self):
        celery = self._app.capp
        celery.control.revoke = MagicMock()
        r = self.post('/api/task/revoke/test', body={})
        self.assertEqual(200, r.code)
        celery.control.revoke.assert_called_once_with('test',
                                                      terminate=False,
                                                      signal='SIGTERM')

    def test_revoke_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.revoke = MagicMock()
            r = self.post('/api/task/revoke/test', body={})
            self.assertEqual(403, r.code)
            celery.control.revoke.assert_not_called()

    def test_terminate(self):
        celery = self._app.capp
        celery.control.revoke = MagicMock()
        r = self.post('/api/task/revoke/test', body={'terminate': True})
        self.assertEqual(200, r.code)
        celery.control.revoke.assert_called_once_with('test',
                                                      terminate=True,
                                                      signal='SIGTERM')

    def test_terminate_read_only(self):
        with patch.object(options.mockable(), 'read_only', True):
            celery = self._app.capp
            celery.control.revoke = MagicMock()
            r = self.post('/api/task/revoke/test', body={'terminate': True})
            self.assertEqual(403, r.code)
            celery.control.revoke.assert_not_called()

    def test_terminate_signal(self):
        celery = self._app.capp
        celery.control.revoke = MagicMock()
        r = self.post('/api/task/revoke/test',
                      body={'terminate': True, 'signal': 'SIGUSR1'})
        self.assertEqual(200, r.code)
        celery.control.revoke.assert_called_once_with('test',
                                                      terminate=True,
                                                      signal='SIGUSR1')


class ControlAuthTests(BaseApiTestCase):
    def test_auth(self):
        with patch.object(options.mockable(), 'basic_auth', ['user1:password1']):
            app = self._app.capp
            app.control.broadcast = MagicMock()
            r = self.post('/api/worker/shutdown/test', body={})
            self.assertEqual(401, r.code)

    @patch.dict(os.environ, {'FLOWER_UNAUTHENTICATED_API': ''})
    def test_auth_without_env_var(self):
        app = self._app.capp
        app.control.broadcast = MagicMock()
        r = self.post('/api/worker/shutdown/test', body={})
        self.assertEqual(401, r.code)
