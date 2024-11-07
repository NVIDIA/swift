# Copyright (c) 2010-2018 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
from random import random

from collections import defaultdict
from eventlet import Timeout
from math import pow

import swift.common.db
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, EventletRateLimiter, \
    non_negative_int, config_float_value
from swift.common.daemon import Daemon
from swift.common.exceptions import DatabaseAuditorException, \
    DatabaseException
from swift.common.recon import DEFAULT_RECON_CACHE_PATH, \
    server_type_to_recon_file


class DatabaseAuditor(Daemon):
    """Base Database Auditor."""

    @property
    def rcache(self):
        return os.path.join(
            self.recon_cache_path,
            server_type_to_recon_file(self.server_type))

    @property
    def server_type(self):
        raise NotImplementedError

    @property
    def broker_class(self):
        raise NotImplementedError

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='{}-auditor'.format(
            self.server_type))
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = float(conf.get('interval', 1800))
        self.logging_interval = 3600  # once an hour
        self.passes = 0
        self.failures = 0
        self._init_vacuum_stats()
        self.max_dbs_per_second = \
            float(conf.get('{}s_per_second'.format(self.server_type), 200))
        self.rate_limiter = EventletRateLimiter(self.max_dbs_per_second)
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.max_freelist_size = non_negative_int(
            conf.get('vacuum_threshold_size', 0))
        self.max_freelist_percent = config_float_value(
            conf.get('vacuum_threshold_percent', 0), 0, 100)
        self.datadir = '{}s'.format(self.server_type)

    def _one_audit_pass(self, reported):
        self._init_vacuum_stats()
        all_locs = audit_location_generator(self.devices, self.datadir, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.audit(path)
            if time.time() - reported >= self.logging_interval:
                self.logger.info(
                    'Since %(time)s: %(server_type)s audits: %(pass)s '
                    'passed audit, %(fail)s failed audit',
                    {'time': time.ctime(reported),
                     'pass': self.passes,
                     'fail': self.failures,
                     'server_type': self.server_type})
                dump_recon_cache(
                    {'{}_audits_since'.format(self.server_type): reported,
                     '{}_audits_passed'.format(self.server_type): self.passes,
                     '{}_audits_failed'.format(self.server_type):
                         self.failures},
                    self.rcache, self.logger)
                reported = time.time()
                self.passes = 0
                self.failures = 0
            self.rate_limiter.wait()
        return reported

    def _init_vacuum_stats(self):
        self.vacuum_stats = {'successes': 0, "failures": 0,
                             'trigger': defaultdict(int)}
        self.freelist_stats = {'percent': defaultdict(int),
                               'size': defaultdict(int)}

    def _write_end_of_pass_stats(self, elapsed):
        self.logger.info(
            '%(server_type)s audit pass completed: %(elapsed).02fs',
            {'elapsed': elapsed, 'server_type': self.server_type.title()})
        dump_recon_cache({
            '{}_auditor_pass_completed'.format(self.server_type): elapsed,
            '{}_vacuum_stats'.format(self.server_type): self.vacuum_stats,
            '{}_freepage_stats'.format(self.server_type): self.freelist_stats},
            self.rcache, self.logger)

    def run_forever(self, *args, **kwargs):
        """Run the database audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(
                'Begin %s audit pass.', self.server_type)
            begin = time.time()
            try:
                reported = self._one_audit_pass(reported)
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception('ERROR auditing')
            elapsed = time.time() - begin
            self._write_end_of_pass_stats(elapsed)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the database audit once."""
        self.logger.info(
            'Begin %s audit "once" mode', self.server_type)
        begin = reported = time.time()
        self._one_audit_pass(reported)
        elapsed = time.time() - begin
        self._write_end_of_pass_stats(elapsed)

    def generate_freepage_stats(self, broker):
        """Generate and send freepage stats.

        This calls into the broker to get free page information. So return
        that information as a dict, so we don't have to bother the broker
        again.
        """
        def get_size_bucket(number):
            lower = 0
            i = 1
            in_mb = int(number / 1024 / 1024)
            while True:
                cur = pow(2, i)
                if cur > in_mb:
                    return lower, int(cur)
                lower = cur
                i += 1

        def get_percent_bucket(number, increment):
            return int(number / increment) * increment

        freelist_size = broker.get_freelist_size()
        lower, upper = get_size_bucket(freelist_size)
        size_stat = "%s.freelist.size.%d-%dMB" % (
            self.server_type, lower, upper - 1)
        self.freelist_stats['size'][size_stat] += 1

        freelist_percent = broker.get_freelist_percent()
        percent_increment = 10
        percent_bucket = get_percent_bucket(
            freelist_percent, percent_increment)
        percent_stat = "%s.freelist.percent.%d-%d" % (
            self.server_type, percent_bucket,
            percent_bucket + (percent_increment - 1))
        self.freelist_stats['percent'][percent_stat] += 1

        return dict(freelist_percent=freelist_percent,
                    freelist_size=freelist_size)

    def generate_freepage_stats(self, broker):
        """Generate and send freepage stats.

        This calls into the broker to get free page information. So return
        that information as a dict, so we don't have to bother the broker
        again.
        """
        def get_size_bucket(number):
            lower = 0
            i = 1
            in_mb = int(number / 1024 / 1024)
            while True:
                cur = pow(2, i)
                if cur > in_mb:
                    return lower, int(cur)
                lower = cur
                i += 1

        def get_percent_bucket(number, increment):
            return int(number / increment) * increment

        freelist_size = broker.get_freelist_size()
        lower, upper = get_size_bucket(freelist_size)
        size_stat = "%s.freelist.size.%d-%dMB" % (
            self.server_type, lower, upper - 1)
        self.logger.increment(size_stat)

        freelist_percent = broker.get_freelist_percent()
        percent_increment = 10
        percent_bucket = get_percent_bucket(
            freelist_percent, percent_increment)
        percent_stat = "%s.freelist.percent.%d-%d" % (
            self.server_type, percent_bucket,
            percent_bucket + (percent_increment - 1))
        self.logger.increment(percent_stat)

        return dict(freelist_percent=freelist_percent,
                    freelist_size=freelist_size)

    def audit(self, path):
        """
        Audits the given database path

        :param path: the path to a db
        """
        start_time = time.time()
        try:
            broker = self.broker_class(path, logger=self.logger)
            if not broker.is_deleted():
                info = broker.get_info()
                err = self._audit(info, broker)
                if err:
                    raise err
                fl_stats = self.generate_freepage_stats(broker)
                vacuum_db = False
                if self.max_freelist_size:
                    # vacuuming is done when freelist_size gets bigger
                    # then the configured value
                    if fl_stats['freelist_size'] > self.max_freelist_size:
                        self.vacuum_stats['trigger']['freelist_size'] += 1
                        self.logger.increment('freelist_size')
                        vacuum_db = True
                if self.max_freelist_percent:
                    # vacuuming is done when freelist_percent gets bigger
                    # then the configured value
                    if fl_stats['freelist_percent'] \
                            > self.max_freelist_percent:
                        self.vacuum_stats['trigger']['freelist_percent'] += 1
                        self.logger.increment('freelist_percent')
                        vacuum_db = True
                if vacuum_db:
                    try:
                        broker.vacuum()
                        self.vacuum_stats['successes'] += 1
                        self.logger.increment('vacuum.successes')
                    except DatabaseException as de:
                        self.logger.warning(
                            'Vacuum failed on %(path)s: %(err)s',
                            {'path': path, 'err': str(de)})
                        self.vacuum_stats['failures'] += 1
                        self.logger.increment('vacuum.failures')
                self.logger.increment('passes')
                self.passes += 1
                self.logger.debug('Audit passed for %s', broker)
        except DatabaseAuditorException as e:
            self.logger.increment('failures')
            self.failures += 1
            self.logger.error('Audit Failed for %(path)s: %(err)s',
                              {'path': path, 'err': str(e)})
        except (Exception, Timeout):
            self.logger.increment('failures')
            self.failures += 1
            self.logger.exception(
                'ERROR Could not get %(server_type)s info %(path)s',
                {'server_type': self.server_type, 'path': path})
        self.logger.timing_since('timing', start_time)

    def _audit(self, info, broker):
        """
        Run any additional audit checks in sub auditor classes

        :param info: The DB <account/container>_info
        :param broker: The broker
        :return: None on success, otherwise an exception to throw.
        """
        raise NotImplementedError
