# Copyright (c) 2024 NVIDIA
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import os
import tempfile
from collections import defaultdict
import functools
import json
import itertools
import logging
import random
import shlex
import time
import unittest
import mock
import six
from six.moves import cStringIO as StringIO

from swift.common.direct_client import DirectClientException
from swift.common.internal_client import InternalClient
from swift.common.swob import HTTPOk, HTTPServerError
from swift.common import utils
from swift.cli import expirer_rebalancer
from swift.obj import expirer

from test.debug_logger import debug_logger
from test.unit import FakeRing, mocked_http_conn, FakeStatus
from test.unit.common.middleware.helpers import FakeSwift


class TestExpirerRebalance(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_PREFIX = b'changeme'
        self.container_ring = FakeRing()
        self.conf = {}
        self.logger = debug_logger()
        self.expirer_config = expirer_rebalancer.ExpirerConfig(
            self.conf, logger=self.logger, container_ring=self.container_ring)
        self.fake_swift = FakeSwift()
        # a *real* ProxyServer app instance creates this in __init__
        self.fake_swift.watchdog = mock.MagicMock()
        self.testdir = tempfile.mkdtemp()
        self.now = utils.Timestamp.now()

    def test_direct_put_container_entry(self):
        target_task_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', 'o')
        task_obj = expirer.build_task_obj(self.now, 'a', 'c', 'o')
        headers = {
            'X-Timestamp': self.now.internal,
            'X-Size': '0',
            'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'X-Content-Type': expirer.X_DELETE_TYPE,
        }
        resp = [201] * 3
        with mocked_http_conn(*resp) as conn:
            expirer_rebalancer.direct_put_container_entry(
                self.container_ring,
                self.expirer_config.account_name,
                target_task_container,
                task_obj,
                headers=headers,
            )
        self.assertEqual(['PUT'] * 3, [r['method'] for r in conn.requests])

    def test_direct_put_container_entry_error(self):
        target_task_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', 'o')
        task_obj_name = expirer.build_task_obj(self.now, 'a', 'c', 'o')
        headers = {
            'X-Timestamp': self.now.internal,
            'X-Size': '0',
            'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'X-Content-Type': expirer.X_DELETE_TYPE,
        }
        resp = [503, 201, 201]
        with mocked_http_conn(*resp) as conn, \
                self.assertRaises(DirectClientException) as ctx:
            expirer_rebalancer.direct_put_container_entry(
                self.container_ring,
                self.expirer_config.account_name,
                target_task_container,
                task_obj_name,
                headers=headers,
            )
        self.assertEqual(['PUT'] * 3, [r['method'] for r in conn.requests])
        msg = str(ctx.exception)
        self.assertIn('Container server', msg)
        self.assertIn('direct PUT', msg)
        self.assertIn('status 503', msg)
        self.assertIn(task_obj_name, msg)
        self.assertIn(target_task_container, msg)

    def _parse_args(self, args=''):
        sh_args = ['/etc/swift/expirer.conf'] + shlex.split(args)
        args = expirer_rebalancer.parser.parse_args(sh_args)
        args.conf = self.conf
        args.logger = self.logger
        return args

    def _run_move_task_object(self, obj, resp, args_str=''):
        target_task_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', obj)
        orig_task_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', 'other-o')
        self.assertNotEqual(target_task_container, orig_task_container)
        task_obj_name = expirer.build_task_obj(self.now, 'a', 'c', obj)
        orig_task_info = {
            'timestamp': self.now,
            'orig_task_obj_name': task_obj_name,
            'content_type': expirer.X_DELETE_TYPE,
            'orig_task_container': orig_task_container,
        }
        args = self._parse_args(args_str)
        worker = expirer_rebalancer.RebalancerWorker(args)
        worker.container_ring = self.container_ring

        with mocked_http_conn(*resp) as conn:
            worker.move_task_object(target_task_container, orig_task_info)
        return worker, conn

    def test_move_task_object(self):
        resp = [201] * 3 + [204] * 3
        _, conn = self._run_move_task_object('o', resp)
        self.assertEqual(['PUT'] * 3 + ['DELETE'] * 3,
                         [r['method'] for r in conn.requests])

    def test_move_task_object_unicode_names(self):
        resp = [201] * 3 + [204] * 3
        _, conn = self._run_move_task_object(u'o1\u2661', resp)
        self.assertEqual(['PUT'] * 3 + ['DELETE'] * 3,
                         [r['method'] for r in conn.requests])
        resp = [201] * 3 + [204] * 3
        _, conn = self._run_move_task_object(u'o2\xf8', resp)
        self.assertEqual(['PUT'] * 3 + ['DELETE'] * 3,
                         [r['method'] for r in conn.requests])

    def test_move_task_object_with_retries(self):
        resp = [201, 201, 503] + [201] * 3 + [204] * 3
        _, conn = self._run_move_task_object('o', resp)
        self.assertEqual(['PUT'] * 6 + ['DELETE'] * 3,
                         [r['method'] for r in conn.requests])

    def test_move_task_object_timeout_with_retries(self):
        slow_status = FakeStatus(201, response_sleep=1.0)
        resp = [201, 201, slow_status] + [201] * 3 + [204] * 3
        args_str = '--timeout 0.1'
        self.assertEqual(0.1, self._parse_args(args_str).timeout)
        worker, conn = self._run_move_task_object('o', resp, args_str=args_str)
        self.assertEqual(['PUT'] * 6 + ['DELETE'] * 3,
                         [r['method'] for r in conn.requests])
        self.assertLess(worker.stats['retry_sleep'], 0.3)

    def test_move_task_object_cleanup_fails(self):
        resp = [210] * 3 + [204, 204, 503]
        _, conn = self._run_move_task_object('o', resp)
        self.assertEqual(['PUT'] * 3 + ['DELETE'] * 3,
                         [r['method'] for r in conn.requests])

    def test_move_task_object_cleanup_retries_timeout(self):
        slow_status = FakeStatus(204, response_sleep=1.0)
        resp = [slow_status] + [201] * 5 + [slow_status] + [204] * 5
        args_str = '--timeout 0.1'
        self.assertEqual(0.1, self._parse_args(args_str).timeout)
        worker, conn = self._run_move_task_object('o', resp, args_str=args_str)
        self.assertEqual(['PUT'] * 6 + ['DELETE'] * 6,
                         [r['method'] for r in conn.requests])
        self.assertLess(worker.stats['retry_sleep'], 0.5)

    def test_move_task_object_fails(self):
        target_task_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', 'o')
        orig_task_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', 'other-o')
        self.assertNotEqual(target_task_container, orig_task_container)
        task_obj_name = expirer.build_task_obj(self.now, 'a', 'c', 'o')
        orig_task_info = {
            'timestamp': self.now,
            'orig_task_obj_name': task_obj_name,
            'content_type': expirer.X_DELETE_TYPE,
            'orig_task_container': orig_task_container,
        }
        resp = [201, 201, 503] * 2
        args = self._parse_args('--retries=1')
        self.assertEqual(1, args.retries)  # sanity
        worker = expirer_rebalancer.RebalancerWorker(args)
        worker.container_ring = self.container_ring
        with mocked_http_conn(*resp) as conn, \
             mock.patch.object(expirer_rebalancer.eventlet, 'sleep'), \
             self.assertRaises(Exception) as ctx:
            worker._move_task_object(target_task_container, orig_task_info)
        self.assertEqual(['PUT'] * 6,
                         [r['method'] for r in conn.requests])
        msg = str(ctx.exception)
        self.assertIn('unable to complete do_migration after 2 errors', msg)
        self.assertIn('DirectClientException', msg)
        self.assertIn('503', msg)
        self.assertIn(task_obj_name, msg)
        self.assertIn(target_task_container, msg)
        self.assertNotIn(orig_task_container, msg)

    def test_set_verosity_level(self):
        args_expected = {
            '': logging.ERROR,
            '-v': logging.INFO,
            '-vv': logging.DEBUG,
            '-vvv': logging.DEBUG,
        }
        for cli_args, expected_level in args_expected.items():
            args = self._parse_args(cli_args)
            args.logger = mock.MagicMock()
            expirer_rebalancer.BaseRebalancer(args)
            if six.PY2:
                expected_calls = args.logger.logger.method_calls
            else:
                expected_calls = args.logger.method_calls
            self.assertEqual([mock.call.setLevel(expected_level)],
                             expected_calls, 'with %r' % cli_args)

    def get_expirer_container(self, obj_name, wrong_container=False):
        right_container = self.expirer_config.get_expirer_container(
            self.now, 'a', 'c', obj_name)
        if wrong_container:
            for i in range(10):
                selected_container = self.expirer_config.get_expirer_container(
                    self.now, 'a', 'wrong-c%d' % i, obj_name)
                if selected_container != right_container:
                    break
            else:
                self.fail('could not hash obj_name into wrong container!?')
        else:
            selected_container = right_container
        return selected_container

    def test_process_task_containers(self):
        now = self.now
        num_stub_task_objs = 32
        task_container_to_obj_name_status = defaultdict(list)
        for i in range(num_stub_task_objs):
            obj_name = 'o%02d' % i
            if random.random() > 0.2:
                wrong_container = True
            else:
                wrong_container = False
            task_container_name = self.get_expirer_container(
                obj_name, wrong_container)
            task_container_to_obj_name_status[task_container_name].append(
                (obj_name, wrong_container))
        stub_task_containers = []
        for task_container, obj_name_status in \
                task_container_to_obj_name_status.items():
            stub_task_containers.append({
                "name": task_container,
                "count": len(obj_name_status),
                "bytes": 0,
                "last_modified": "2024-10-17T18:10:07.076510",
                'rebalance_day_delta': 0,
            })
            task_container_listing = [{
                "bytes": 0,
                "hash": "d41d8cd98f00b204e9800998ecf8427e",
                "name": expirer.build_task_obj(now, 'a', 'c', obj_name),
                "content_type": "text/plain;swift_expirer_bytes=8",
                "last_modified": now.isoformat,
            } for obj_name, status in obj_name_status]
            task_container_path = \
                '/v1/.expiring_objects/%s' % task_container
            self.fake_swift.register(
                'GET', task_container_path, HTTPOk, {},
                json.dumps(task_container_listing))
            self.fake_swift.register_next_response(
                'GET', task_container_path, HTTPOk, {},
                json.dumps([]))
        # py36 needs the verbosity up, so the logLevel of DEBUG (or at least
        # INFO can be set in the debug_logger.
        args = self._parse_args('--concurrency 1 -vvv')
        # a for-realzy ExpirerRebalancer uses a mp.Queue here, but this test is
        # in eventlet land and the interface is compatible
        result_q = expirer_rebalancer.eventlet.Queue()
        fake_swift_ic = functools.partial(InternalClient, app=self.fake_swift)

        moved_tasks = []

        def capture_task_object(args, target_task_container, orig_task_info):
            moved_tasks.append((target_task_container, orig_task_info))

        with mock.patch.object(expirer_rebalancer, 'InternalClient',
                               fake_swift_ic), \
                mock.patch.object(expirer_rebalancer.RebalancerWorker,
                                  '_move_task_object', capture_task_object):
            expirer_rebalancer.process_task_containers(
                args, stub_task_containers, result_q)

        stats = result_q.get()
        total_wrong_task = sum(
            is_wrong
            for container, obj_name_status
            in task_container_to_obj_name_status.items()
            for obj_name, is_wrong in obj_name_status)
        self.assertEqual({
            'processed_tasks': total_wrong_task,
            'success_tasks': total_wrong_task,
        }, stats)

        prefix = expirer_rebalancer._get_worker_prefix()
        info_lines = self.logger.get_lines_for_level('info')
        task_message_template = \
            '%(prefix)s found %(num_wrong)s mis-queued tasks in %(container)s'
        # XXX should this be un-ordered or is eventlet stable?
        self.maxDiff = None
        self.assertEqual([
            task_message_template % {
                'prefix': prefix,
                'num_wrong': sum(s[1] for s in obj_name_status),
                'container': task_container,
            }
            for task_container, obj_name_status
            in task_container_to_obj_name_status.items()
        ], info_lines[:len(task_container_to_obj_name_status)])
        expected_finish_templates = [
            '%(prefix)s listed %(num_container)s task_containers and '
            'queued %(wrong_task)s/%(num_task)s tasks',
            '%(prefix)s waiting on greenthreads',
        ]
        self.assertEqual([
            template % {
                'prefix': prefix,
                'num_container': len(task_container_to_obj_name_status),
                'wrong_task': total_wrong_task,
                'num_task': num_stub_task_objs,
            } for template in expected_finish_templates
        ], info_lines[-2:])

        # all wrong_container tasks are processed
        for task_container, orig_task_info in moved_tasks:
            ts, a, c, o = expirer_rebalancer.parse_task_obj(
                orig_task_info['orig_task_obj_name'])
            task_container_to_obj_name_status[
                orig_task_info['orig_task_container']].remove((o, True))
        self.assertFalse(any(status
                             for task_container, obj_name_status
                             in task_container_to_obj_name_status.items()
                             for obj_name, status in obj_name_status),
                         task_container_to_obj_name_status)

    def test_process_q_moves_on(self):
        now = self.now
        task_container_name = self.get_expirer_container(
            'o', wrong_container=True)
        num_stub_task_objs = 8
        num_bad_tasks_objs = 2
        task_container_listing = []
        for i in range(num_stub_task_objs):
            obj_name = 'o%02d' % i
            task_container_listing.append({
                "bytes": 0,
                "hash": "d41d8cd98f00b204e9800998ecf8427e",
                "name": expirer.build_task_obj(now, 'a', 'c', obj_name),
                "content_type": "text/plain;swift_expirer_bytes=8",
                "last_modified": now.isoformat,
            })
        task_container_path = \
            '/v1/.expiring_objects/%s' % task_container_name
        self.fake_swift.register(
            'GET', task_container_path, HTTPOk, {},
            json.dumps(task_container_listing))
        self.fake_swift.register_next_response(
            'GET', task_container_path, HTTPOk, {},
            json.dumps([]))

        bad_tasks = set(
            expirer.build_task_obj(now, 'a', 'c', 'o%02d' % i)
            for i in random.sample(list(range(num_stub_task_objs)),
                                   num_bad_tasks_objs)
        )

        args = self._parse_args('--concurrency 2')
        # a for-realzy ExpirerRebalancer uses a mp.Queue here, but this test is
        # in eventlet land and the interface is compatible
        result_q = expirer_rebalancer.eventlet.Queue()
        fake_swift_ic = functools.partial(InternalClient, app=self.fake_swift)

        def exploding_move_task_object(args, target_task_container,
                                       orig_task_info):
            if orig_task_info['orig_task_obj_name'] in bad_tasks:
                raise Exception('kaboom!')

        stub_task_containers = [{
            "name": task_container_name,
            "count": num_stub_task_objs,
            "bytes": 0,
            "last_modified": "2024-10-17T18:10:07.076510",
            'rebalance_day_delta': 0,
        }]
        with mock.patch.object(expirer_rebalancer, 'InternalClient',
                               fake_swift_ic), \
                mock.patch.object(expirer_rebalancer.RebalancerWorker,
                                  '_move_task_object',
                                  exploding_move_task_object):
            expirer_rebalancer.process_task_containers(
                args, stub_task_containers, result_q)

        stats = result_q.get()
        self.assertEqual({
            'processed_tasks': num_stub_task_objs,
            'success_tasks': num_stub_task_objs - num_bad_tasks_objs,
            'failed_tasks': num_bad_tasks_objs,
        }, stats)

    def test_process_task_container_limit(self):
        task_container_name = self.get_expirer_container(
            'o', wrong_container=True)
        num_stub_task_objs = 8
        task_container_listing = []
        for i in range(num_stub_task_objs):
            obj_name = 'o%02d' % i
            task_container_listing.append({
                "bytes": 0,
                "hash": "d41d8cd98f00b204e9800998ecf8427e",
                "name": expirer.build_task_obj(self.now, 'a', 'c', obj_name),
                "content_type": "text/plain;swift_expirer_bytes=8",
                "last_modified": self.now.isoformat,
            })
        task_container_path = \
            '/v1/.expiring_objects/%s' % task_container_name
        self.fake_swift.register(
            'GET', task_container_path, HTTPOk, {},
            json.dumps(task_container_listing))
        self.fake_swift.register_next_response(
            'GET', task_container_path, HTTPOk, {},
            json.dumps([]))

        # py36 needs the verbosity up, so the logLevel of DEBUG (or at least
        # INFO can be set in the debug_logger.
        args = self._parse_args('--limit 3 -vvv')
        # a for-realzy ExpirerRebalancer uses a mp.Queue here, but this test is
        # in eventlet land and the interface is compatible
        result_q = expirer_rebalancer.eventlet.Queue()
        fake_swift_ic = functools.partial(InternalClient, app=self.fake_swift)

        args.logger.setLevel(logging.DEBUG)
        stub_task_containers = [{
            "name": task_container_name,
            "count": num_stub_task_objs,
            "bytes": 0,
            "last_modified": "2024-10-17T18:10:07.076510",
            'rebalance_day_delta': 0,
        }]
        with mock.patch.object(expirer_rebalancer, 'InternalClient',
                               fake_swift_ic), \
                mock.patch.object(expirer_rebalancer.RebalancerWorker,
                                  '_move_task_object'):
            expirer_rebalancer.process_task_containers(
                args, stub_task_containers, result_q)

        stats = result_q.get()
        self.assertEqual({
            'processed_tasks': 3,
            'success_tasks': 3,
        }, stats)

        prefix = expirer_rebalancer._get_worker_prefix()
        expected_message_templates = [
            '%(prefix)s found %(limit)s mis-queued tasks in %(container)s',
            '%(prefix)s listed 1 task_containers and '
            'queued %(limit)s/%(num_task)s tasks',
            '%(prefix)s waiting on greenthreads',
        ]
        self.assertEqual([
            template % {
                'prefix': prefix,
                'container': task_container_name,
                'limit': 3,
                'num_task': num_stub_task_objs,
            } for template in expected_message_templates
        ], self.logger.get_lines_for_level('info'))

    def test_process_task_containers_errors(self):
        num_task_containers = 3
        task_container_names = set()
        name_iter = ('obj%03d' % i for i in itertools.count())
        while len(task_container_names) < num_task_containers:
            obj_name = next(name_iter)
            task_container_names.add(self.get_expirer_container(obj_name))
        # this makes it shuffle-able; we'll sort it later
        task_container_names = list(task_container_names)
        task_container_to_obj_names = {
            task_container: []
            for task_container in task_container_names
        }
        # put all the tasks in the wrong containers, ensuring there are more
        # than listing_page_split in each container so that a second page
        # listing will be attempted for each container
        listing_page_split = 4
        num_stub_task_objs = 0
        while (num_stub_task_objs <= 32
               or any([len(objs) <= listing_page_split
                       for objs in task_container_to_obj_names.values()])):
            obj_name = next(name_iter)
            right_container_name = self.get_expirer_container(obj_name)
            random.shuffle(task_container_names)
            for name in task_container_names:
                if name != right_container_name:
                    wrong_task_container_name = name
                    break
            else:
                self.fail('more than one right container name!?')
            task_container_to_obj_names[wrong_task_container_name].append(
                obj_name)
            num_stub_task_objs += 1

        # each task_container will be fully listed in 2 pages followed by the
        # empty list response except the 2nd container; it's going to blow up
        # after the first page
        bad_container_index = 1

        task_container_names.sort()
        stub_task_containers = []
        for task_container in task_container_names:
            obj_names = task_container_to_obj_names[task_container]
            self.assertGreater(len(obj_names), listing_page_split)  # sanity
            stub_task_containers.append({
                "name": task_container,
                "count": len(obj_names),
                "bytes": 0,
                "last_modified": "2024-10-17T18:10:07.076510",
                'rebalance_day_delta': 0,
            })
            task_container_listing1 = [{
                "bytes": 0,
                "hash": "d41d8cd98f00b204e9800998ecf8427e",
                "name": expirer.build_task_obj(self.now, 'a', 'c', obj_name),
                "content_type": "text/plain;swift_expirer_bytes=8",
                "last_modified": self.now.isoformat,
            } for obj_name in obj_names[:listing_page_split]]
            task_container_listing2 = [{
                "bytes": 0,
                "hash": "d41d8cd98f00b204e9800998ecf8427e",
                "name": expirer.build_task_obj(self.now, 'a', 'c', obj_name),
                "content_type": "text/plain;swift_expirer_bytes=8",
                "last_modified": self.now.isoformat,
            } for obj_name in obj_names[listing_page_split:]]
            task_container_path = \
                '/v1/.expiring_objects/%s' % task_container
            self.fake_swift.register(
                'GET', task_container_path, HTTPOk, {},
                json.dumps(task_container_listing1))
            if task_container == task_container_names[bad_container_index]:
                self.fake_swift.register_next_response(
                    'GET', task_container_path, HTTPServerError, {},
                    b'Try Again')
            else:
                # the others will list fine
                self.fake_swift.register_next_response(
                    'GET', task_container_path, HTTPOk, {},
                    json.dumps(task_container_listing2))
                self.fake_swift.register_next_response(
                    'GET', task_container_path, HTTPOk, {},
                    json.dumps([]))
        args = self._parse_args('--retries 1')
        # a for-realzy ExpirerRebalancer uses a mp.Queue here, but this test is
        # in eventlet land and the interface is compatible
        result_q = expirer_rebalancer.eventlet.Queue()
        fake_swift_ic = functools.partial(InternalClient, app=self.fake_swift)

        moved_tasks = []

        def capture_task_object(args, target_task_container, orig_task_info):
            moved_tasks.append((target_task_container, orig_task_info))

        with mock.patch.object(expirer_rebalancer, 'InternalClient',
                               fake_swift_ic), \
                mock.patch.object(expirer_rebalancer.RebalancerWorker,
                                  '_move_task_object', capture_task_object):
            expirer_rebalancer.process_task_containers(
                args, stub_task_containers, result_q)

        stats = result_q.get()
        # entries in the first (successful) listing page are still queued
        expected_tasks = num_stub_task_objs - len(
            task_container_to_obj_names[
                task_container_names[bad_container_index]
            ]) + listing_page_split
        self.assertEqual({
            'processed_tasks': expected_tasks,
            'success_tasks': expected_tasks,
        }, stats)

        self.assertEqual([
            '%(prefix)s error listing tasks in %(task_container)s' % {
                'prefix': expirer_rebalancer._get_worker_prefix(),
                'task_container': stub_task_containers[bad_container_index],
            }
        ], self.logger.get_lines_for_level('error'))

    def _get_ic(self):
        return InternalClient('', 'test-expirer-rebalance', 1,
                              app=self.fake_swift)

    def test_get_task_containers_to_migrate(self):
        t0 = time.time()
        tomorrow = t0 + 86400
        day_after_tomorrow = tomorrow + 86400
        task_containers = sorted([
            self.expirer_config.get_expirer_container(
                t0, 'a', 'c', 'o0'),
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o1'),
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o2'),
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o3'),
            self.expirer_config.get_expirer_container(
                day_after_tomorrow, 'a', 'c', 'o4'),
        ])
        self.assertEqual(len(task_containers),
                         len(set(task_containers)))  # sanity, no dupes
        task_container_listing = [{
            "name": task_container,
            "count": 1,
            "bytes": 0,
            "last_modified": "2024-10-17T18:10:07.076510"
        } for task_container in task_containers]
        swift = self._get_ic()
        self.fake_swift.register(
            'GET', '/v1/.expiring_objects', HTTPOk, {},
            json.dumps(task_container_listing))
        self.fake_swift.register_next_response(
            'GET', '/v1/.expiring_objects', HTTPOk, {},
            json.dumps([]))

        # py36 needs the verbosity up, so the logLevel of DEBUG (or at least
        # INFO can be set in the debug_logger.
        args = self._parse_args('-vvv')
        rebalancer = expirer_rebalancer.ExpirerRebalancer(args)
        result = rebalancer._find_task_containers(
            swift, args.start_day_offset, args.num_days)
        # XXX is day_delta still 0 when this test runs right after UTC 00:00?
        expected = [dict(c, rebalance_day_delta=0)
                    for c in task_container_listing[1:-1]]
        self.assertEqual(expected, result)
        expected_message_templates = [
            'skipping task_container %s due in -1 days with 1 tasks',
            'found task_container %s due in 0 days with 1 tasks',
            'found task_container %s due in 0 days with 1 tasks',
            'found task_container %s due in 0 days with 1 tasks',
            'stopping at task_container %s due in 1 days with 1 tasks',
        ]
        self.assertEqual({'info': [
            template % task_container
            for template, task_container in zip(
                expected_message_templates, task_containers)
        ]}, self.logger.all_log_lines())

    def test_get_task_containers_to_migrate_moar_dayz(self):
        t0 = time.time()
        tomorrow = t0 + 86400
        day_after_tomorrow = tomorrow + 86400
        task_containers = sorted([
            self.expirer_config.get_expirer_container(
                t0, 'a', 'c', 'o0'),
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o1'),
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o2'),
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o3'),
            self.expirer_config.get_expirer_container(
                day_after_tomorrow, 'a', 'c', 'o4'),
        ])
        self.assertEqual(len(task_containers),
                         len(set(task_containers)))  # sanity, no dupes
        task_container_listing = [{
            "name": task_container,
            "count": 1,
            "bytes": 0,
            "last_modified": "2024-10-17T18:10:07.076510"
        } for task_container in task_containers]
        swift = self._get_ic()
        self.fake_swift.register(
            'GET', '/v1/.expiring_objects', HTTPOk, {},
            json.dumps(task_container_listing))
        self.fake_swift.register_next_response(
            'GET', '/v1/.expiring_objects', HTTPOk, {},
            json.dumps([]))
        args = self._parse_args('--start-day-offset -1 --num-days 3')
        rebalancer = expirer_rebalancer.ExpirerRebalancer(args)
        result = rebalancer._find_task_containers(
            swift, args.start_day_offset, args.num_days)
        expected = [
            dict(c, rebalance_day_delta=delta)
            for c, delta in zip(task_container_listing, [-1, 0, 0, 0, 1])
        ]
        self.maxDiff = None
        self.assertEqual(expected, result)

    def test_main_invalid_workers(self):
        out = StringIO()
        err = StringIO()
        with self.assertRaises(SystemExit) as context, \
                mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            expirer_rebalancer.main(['invalid.conf', '--workers', '0'])

        self.assertEqual(context.exception.code, SystemExit(2).code)
        # The number of workers must be greater than 0
        self.assertIn("error: argument --workers: invalid "
                      "config_positive_int_value value: '0'",
                      err.getvalue().splitlines()[-1])

    def test_main_return_on_errors(self):
        conf_path = os.path.join(self.testdir, 'object-expirer.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[object-expirer]')

        with mock.patch('sys.stdout', new=StringIO()) as mock_stdout, \
            mock.patch.object(expirer_rebalancer.ExpirerRebalancer,
                              'get_task_containers_to_migrate') as mock_get:
            mock_get.side_effect = Exception('test error')
            ret = expirer_rebalancer.main([conf_path])

        self.assertEqual(1, ret)
        self.assertEqual('done with errors, please check logs.\n',
                         mock_stdout.getvalue())

        with mock.patch('sys.stdout', new=StringIO()) as mock_stdout, \
            mock.patch.object(expirer_rebalancer.ExpirerRebalancer,
                              'run_workers') as mock_run_workers:
            mock_run_workers.return_value = {'failed_tasks': 1}
            ret = expirer_rebalancer.main([conf_path])

        self.assertEqual(1, ret)
        self.assertEqual('done with errors, please check logs.\n',
                         mock_stdout.getvalue())

    def test_main(self):
        # attempting to test main to sanity check config/parser handling might
        # reasonable, but without careful patching to prevent forking and
        # spawning mp threads without closing the leaky eventlet context that
        # exists in a test runner is dubious
        tomorrow = utils.Timestamp(time.time() + 86396)
        num_task_containers = 1
        num_task_per_task_container = 3
        task_container_names = [
            self.expirer_config.get_expirer_container(
                tomorrow, 'a', 'c', 'o%04d' % i)
            for i in range(num_task_containers)
        ]
        task_container_listing = [{
            'count': num_task_per_task_container,
            'name': task_container_name
        } for task_container_name in task_container_names]
        self.fake_swift.register(
            'GET', '/v1/.expiring_objects', HTTPOk, {},
            json.dumps(task_container_listing))
        self.fake_swift.register_next_response(
            'GET', '/v1/.expiring_objects', HTTPOk, {},
            json.dumps([]))
        fake_swift_ic = functools.partial(InternalClient, app=self.fake_swift)
        conf_path = os.path.join(self.testdir, 'object-expirer.conf')
        with open(conf_path, 'w') as fd:
            fd.write('[object-expirer]')
        with mock.patch.object(expirer_rebalancer, 'InternalClient',
                               fake_swift_ic), \
            mock.patch.object(expirer_rebalancer.ExpirerRebalancer,
                              'run_workers') as mock_run_workers:
            mock_run_workers.return_value = {}
            ret = expirer_rebalancer.main([conf_path])
        self.assertEqual(mock_run_workers.call_args_list, [
            mock.call(mock.ANY, [{
                'count': num_task_per_task_container,
                'name': task_container_name,
                'rebalance_day_delta': 0,
            } for task_container_name in task_container_names]),
        ])
        self.assertEqual(0, ret)
