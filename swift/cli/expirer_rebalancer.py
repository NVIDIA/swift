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

from argparse import ArgumentParser
import contextlib
from collections import defaultdict
import datetime
import eventlet
import logging
import multiprocessing as mp
import os
import queue
import sys
import time
import six

from swift.common.direct_client import direct_put_container_object
from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.request_helpers import USE_REPLICATION_NETWORK_HEADER
from swift.common.utils.config import readconf, config_positive_int_value
from swift.common.utils import get_prefixed_logger
from swift.common.utils.timestamp import Timestamp
from swift.common.utils import (get_logger, eventlet_monkey_patch,
                                distribute_evenly)
from swift.container.reconciler import direct_delete_container_entry
from swift.obj.expirer import parse_task_obj, ExpirerConfig


parser = ArgumentParser()
parser.add_argument('expirer_conf_path', help='path to expirer config')
# common options
parser.add_argument('--retries', type=int, default=3,
                    help='allow some errors to be retried')
parser.add_argument('-v', '--verbose', action='count', default=0,
                    help='increase verbosity')
# main options
parser.add_argument('--workers', type=config_positive_int_value, default=3,
                    help='listing worker processes')
parser.add_argument('--start-day-offset', type=int, default=0,
                    help="how many days from today (positive or negative) "
                    "to start the task_container search for movable tasks")
parser.add_argument('--num-days', type=int, default=1,
                    help="How many task_container days to move")
# worker options
parser.add_argument('--concurrency', type=int, default=10,
                    help='number of concurrent tasks per worker')
parser.add_argument('--timeout', type=float, default=5.0,
                    help='how many second(s) to give direct operations')
parser.add_argument('--limit-per-task-container', type=int, default=None,
                    help='Only process a few task entries per task_container')
parser.add_argument('--dry-run', action='store_true',
                    help='do not move anything')


def direct_put_container_entry(container_ring, account_name, container_name,
                               object_name, headers=None):
    """
    Write directly to primary container servers to create a new expirer task.
    """
    headers = headers or {}
    headers[USE_REPLICATION_NETWORK_HEADER] = 'true'

    part, nodes = container_ring.get_nodes(account_name, container_name)
    pile = eventlet.GreenPile()
    for node in nodes:
        pile.spawn(direct_put_container_object, node, part, account_name,
                   container_name, object_name, headers=headers)
    # if any primaries fail this will raise an exception
    for v in pile:
        pass


def conf_from_args(args):
    if hasattr(args, 'conf'):
        # N.B. just for tests
        conf = args.conf
    else:
        conf = readconf(args.expirer_conf_path, 'object-expirer',
                        log_name='expirer-rebalancer')
    return conf


class BaseRebalancer(object):

    def __init__(self, args):
        self.conf = conf_from_args(args)
        self.logger = self.get_logger(args)
        self.expirer_config = ExpirerConfig(self.conf, logger=self.logger)
        self.internal_client_conf_path = self.conf.get(
            'internal_client_conf_path', '/etc/swift/internal-client.conf')
        self.retries = args.retries

    def get_logger(self, args):
        """
        Base method so Workers can override to add a prefix.
        """
        if hasattr(args, 'logger'):
            # N.B. just for tests
            logger = args.logger
        else:
            logger = get_logger(
                self.conf, log_to_console=args.verbose > 0)
        verbose_map = {
            0: logging.ERROR,
            1: logging.INFO,
            2: logging.DEBUG
        }
        log_level = verbose_map.get(args.verbose, logging.DEBUG)
        if six.PY2:
            logger.logger.setLevel(log_level)
        else:
            logger.setLevel(log_level)
        return logger

    @contextlib.contextmanager
    def internal_client_ctx(self):
        """
        This is mostly trying to keep eventlet monkey patching isolated from
        the multiprocess Queue to avoid deadlocks.  The consistency of
        user_agent and log_name configuration is just a side-effect.
        """
        eventlet_monkey_patch()
        user_agent = 'expirer-rebalancer'
        log_name = 'expirer-rebalancer-ic'
        ic = InternalClient(self.internal_client_conf_path, user_agent,
                            self.retries, use_replication_network=True,
                            global_conf={'log_name': log_name})
        yield ic
        # we're done with the eventlet silliness!
        ic.app._pipeline_final_app.watchdog._run_gth.kill()
        eventlet.hubs.get_hub().abort(wait=True)


def _get_worker_prefix():
    # pulled out so tests can consistently expect the prefix in worker logs
    return '[%s pid=%d]' % (mp.current_process().name, os.getpid())


class RebalancerWorker(BaseRebalancer):

    def __init__(self, args):
        super(RebalancerWorker, self).__init__(args)
        self.dry_run = args.dry_run
        self.concurrency = args.concurrency
        self.timeout = args.timeout
        self.limit_per_task_container = args.limit_per_task_container
        self.stats = defaultdict(int)

    def get_logger(self, args):
        logger = super(RebalancerWorker, self).get_logger(args)
        worker_logger = get_prefixed_logger(
            logger, '%s ' % _get_worker_prefix())
        return worker_logger

    def _retry(self, func):
        start = time.time()
        errors = []
        while True:
            try:
                return func()
            except (Exception, eventlet.Timeout) as e:
                errors.append(e)
            if len(errors) <= self.retries:
                elapsed = time.time() - start
                sleep_time = elapsed * 2.0
                self.stats['retry_attempts'] += 1
                self.stats['retry_sleep'] += sleep_time
                eventlet.sleep(sleep_time)
            else:
                raise Exception('unable to complete %s after %s errors: %r' % (
                    func.__name__, len(errors), errors))

    def _move_task_object(self, target_task_container, orig_task_info):
        # put entry in new target_task_container
        headers = {
            'X-Timestamp': orig_task_info['timestamp'].internal,
            'X-Size': '0',
            'X-Etag': 'd41d8cd98f00b204e9800998ecf8427e',
            'X-Content-Type': orig_task_info['content_type'],
        }
        self.logger.debug('moving %r to %s w/ %r', orig_task_info,
                          target_task_container, headers)
        if self.dry_run:
            return

        # create a copy of the task in the correct task_container
        def do_migration():
            try:
                with eventlet.Timeout(seconds=self.timeout):
                    direct_put_container_entry(
                        self.container_ring,
                        self.expirer_config.account_name,
                        target_task_container,
                        orig_task_info['orig_task_obj_name'], headers=headers)
            except eventlet.Timeout:
                self.logger.error('timeout writing %r to %r w/ %r',
                                  orig_task_info,
                                  target_task_container,
                                  headers)
                raise
        self._retry(do_migration)

        # pop queue
        def do_cleanup():
            try:
                with eventlet.Timeout(seconds=self.timeout):
                    direct_delete_container_entry(
                        self.container_ring, self.expirer_config.account_name,
                        orig_task_info['orig_task_container'],
                        orig_task_info['orig_task_obj_name'])
            except eventlet.Timeout:
                self.logger.error('timeout cleaning %r from %r',
                                  orig_task_info['orig_task_obj_name'],
                                  orig_task_info['orig_task_container'])
                raise
        self._retry(do_cleanup)

    def move_task_object(self, target_task_container, orig_task_info):
        self.stats['processed_tasks'] += 1
        try:
            self._move_task_object(target_task_container, orig_task_info)
        except Exception:
            self.logger.error('unable to move %r to %r' % (
                orig_task_info, target_task_container))
            self.stats['failed_tasks'] += 1
        else:
            self.stats['success_tasks'] += 1

    def process_task_container(self, swift, task_container, pool):
        """
        Iterate task objects in the given container and spawn a
        move_task_object into the pool for any that are mis-queued.
        """
        moved_tasks = 0
        for task_obj in swift.iter_objects(
                self.expirer_config.account_name, task_container['name']):
            timestamp, target_account, target_container, target_obj = \
                parse_task_obj(task_obj['name'])
            expected_task_container = \
                self.expirer_config.get_expirer_container(
                    timestamp, target_account, target_container, target_obj)
            if expected_task_container != task_container['name']:
                orig_task_info = {
                    'orig_task_container': task_container['name'],
                    'x_delete_at': timestamp,
                    'account': target_account,
                    'container': target_container,
                    'obj': target_obj,
                    'content_type': task_obj['content_type'],
                    'timestamp': Timestamp.from_isoformat(
                        task_obj['last_modified']),
                    'orig_task_obj_name': task_obj['name'],
                }
                moved_tasks += 1
                move_task_args = (expected_task_container, orig_task_info)
                pool.spawn_n(self.move_task_object, *move_task_args)
                if (self.limit_per_task_container and
                        moved_tasks >= self.limit_per_task_container):
                    break
        self.logger.info('found %s mis-queued tasks in %s',
                         moved_tasks, task_container['name'])
        return moved_tasks

    @contextlib.contextmanager
    def internal_client_ctx(self):
        """
        Wrap the base method to steal container_ring for direct requests
        """
        with super(RebalancerWorker, self).internal_client_ctx() as ic:
            self.container_ring = ic.container_ring
            yield ic
            self.container_ring = None

    def process(self, task_containers):
        with self.internal_client_ctx() as swift:
            pool = eventlet.GreenPool(self.concurrency)
            worker_tasks = 0
            for task_container in task_containers:
                try:
                    worker_tasks += self.process_task_container(
                        swift, task_container, pool)
                except UnexpectedResponse:
                    self.logger.error(
                        'error listing tasks in %s' % task_container)

            self.logger.info('listed %s task_containers '
                             'and queued %s/%s tasks',
                             len(task_containers),
                             worker_tasks,
                             sum(c['count'] for c in task_containers))
            self.logger.info('waiting on greenthreads')
            pool.waitall()


def process_task_containers(args, task_containers, result_q):
    rebalance_worker = RebalancerWorker(args)
    rebalance_worker.process(task_containers)
    result_q.put(dict(rebalance_worker.stats))


class ExpirerRebalancer(BaseRebalancer):

    def __init__(self, args):
        super(ExpirerRebalancer, self).__init__(args)
        # technically run_workers requires args, but those are ostensibly just
        # for "passing through" to process_task_containers
        self.workers = args.workers

    def _find_task_containers(self, swift, start_day_offset, num_days):
        now = datetime.datetime.now()
        task_containers = []
        for task_container in swift.iter_containers(
                self.expirer_config.account_name):
            container_dt = datetime.datetime.fromtimestamp(
                int(task_container['name']) +
                self.expirer_config.task_container_per_day)
            day_delta = task_container['rebalance_day_delta'] = (
                container_dt - now).days
            if day_delta < start_day_offset:
                self.logger.info('skipping task_container %(name)s due in '
                                 '%(rebalance_day_delta)s days '
                                 'with %(count)s tasks', task_container)
                continue
            if day_delta >= start_day_offset + num_days:
                self.logger.info('stopping at task_container %(name)s due in '
                                 '%(rebalance_day_delta)s days '
                                 'with %(count)s tasks', task_container)
                break
            self.logger.info('found task_container %(name)s due in '
                             '%(rebalance_day_delta)s days '
                             'with %(count)s tasks', task_container)
            task_containers.append(task_container)
        return task_containers

    def get_task_containers_to_migrate(self, start_day_offset, num_days):
        with self.internal_client_ctx() as swift:
            task_containers = self._find_task_containers(
                swift, start_day_offset, num_days)
        self.logger.info(
            'found %s task_containers w/ %s tasks to evaluate',
            len(task_containers),
            sum(c['count'] for c in task_containers))
        return task_containers

    def _spawn_workers(self, args, task_containers, result_q):
        workers = []
        for i, batch in enumerate(distribute_evenly(task_containers,
                                                    self.workers)):
            w = mp.Process(
                name='worker %d/%d' % (i, self.workers),
                target=process_task_containers,
                args=(args, batch, result_q)
            )
            w.start()
            workers.append(w)
        return workers

    def _get_queue(self):
        return mp.Queue(self.workers)

    def run_workers(self, args, task_containers):
        result_q = self._get_queue()
        workers = self._spawn_workers(args, task_containers, result_q)
        # XXX this can deadlock if there's a lot of workers that finish early
        # and the result_q gets full
        self.logger.info('waiting on listing workers')
        for worker in workers:
            worker.join()

        aggregate_result = defaultdict(int)
        while True:
            try:
                worker_result = result_q.get(block=False)
            except queue.Empty:
                break
            for k, v in worker_result.items():
                aggregate_result[k] += v
        return dict(aggregate_result)


def main(args=None):
    args = parser.parse_args(args)
    expirer_rebalancer = ExpirerRebalancer(args)
    err_msg = 'done with errors, please check logs.'

    try:
        task_containers = expirer_rebalancer.get_task_containers_to_migrate(
            args.start_day_offset, args.num_days)
    except Exception as e:
        expirer_rebalancer.logger.exception(
            'Failed to get expirering task containers: %s', e)
        print(err_msg)
        return 1

    stats = expirer_rebalancer.run_workers(args, task_containers)
    for k, v in stats.items():
        print(k, v)
    if stats.get('failed_tasks', 0) > 0:
        print(err_msg)
        return 1
    print('done')
    return 0


if __name__ == "__main__":
    sys.exit(main())
