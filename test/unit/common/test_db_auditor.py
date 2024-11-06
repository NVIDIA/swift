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

import unittest
from unittest import mock
import time
import os
import random
import re
from tempfile import mkdtemp
from shutil import rmtree
from eventlet import Timeout

from swift.common.db_auditor import DatabaseAuditor
from swift.common.exceptions import DatabaseException
from test.debug_logger import debug_logger


class FakeDatabaseBroker(object):
    def __init__(self, path, logger):
        self.path = path
        self.db_file = path
        self.file = os.path.basename(path)
        self.re_match = re.compile(r'^[a-zA-Z]*([0-9]*).db$')

    def is_deleted(self):
        return False

    def get_info(self):
        if self.file.startswith('fail'):
            raise ValueError
        if self.file.startswith('true'):
            return 'ok'

    def get_freelist_size(self):
        number = self.re_match.match(self.file).group(1)
        return int(number) if number else 0

    def get_freelist_percent(self):
        number = self.re_match.match(self.file).group(1)
        # make them a bigger percent
        return int(number) * 10 if number else 0

    def vacuum(self):
        # odd numbers fail to vacuum (just for stats)
        if self.get_freelist_size() % 2 == 0:
            return
        else:
            raise DatabaseException('Vacuum failed!')


class FakeDatabaseAuditor(DatabaseAuditor):
    server_type = "container"
    broker_class = FakeDatabaseBroker

    def _audit(self, info, broker):
        return None


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_database_auditor')
        self.logger = debug_logger()
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        fnames = ['true1.db', 'true2.db', 'true3.db',
                  'fail1.db', 'fail2.db']
        for fn in fnames:
            with open(os.path.join(self.testdir, fn), 'w+') as f:
                f.write(' ')

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_run_forever(self):
        sleep_times = random.randint(5, 10)
        call_times = sleep_times - 1

        class FakeTime(object):
            def __init__(self):
                self.times = 0

            def sleep(self, sec):
                self.times += 1
                if self.times < sleep_times:
                    time.sleep(0.1)
                else:
                    # stop forever by an error
                    raise ValueError()

            def time(self):
                return time.time()

        conf = {}
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)

        with mock.patch('swift.common.db_auditor.time', FakeTime()):
            def fake_audit_location_generator(*args, **kwargs):
                files = os.listdir(self.testdir)
                return [(os.path.join(self.testdir, f), '', '') for f in files]

            with mock.patch('swift.common.db_auditor.audit_location_generator',
                            fake_audit_location_generator):
                self.assertRaises(ValueError, test_auditor.run_forever)
        self.assertEqual(test_auditor.failures, 2 * call_times)
        self.assertEqual(test_auditor.passes, 3 * call_times)

        # now force timeout path code coverage
        with mock.patch('swift.common.db_auditor.DatabaseAuditor.'
                        '_one_audit_pass', side_effect=Timeout()):
            with mock.patch('swift.common.db_auditor.time', FakeTime()):
                self.assertRaises(ValueError, test_auditor.run_forever)

    def test_run_once(self):
        conf = {}
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = os.listdir(self.testdir)
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        with mock.patch('swift.common.db_auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor.run_once()
        self.assertEqual(test_auditor.failures, 2)
        self.assertEqual(test_auditor.passes, 3)

    def test_one_audit_pass(self):
        conf = {}
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = sorted(os.listdir(self.testdir))
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        # force code coverage for logging path
        with mock.patch('swift.common.db_auditor.audit_location_generator',
                        fake_audit_location_generator), \
                mock.patch('time.time',
                           return_value=(test_auditor.logging_interval * 2)):
            test_auditor._one_audit_pass(0)
        self.assertEqual(test_auditor.failures, 1)
        self.assertEqual(test_auditor.passes, 3)

    def test_database_auditor(self):
        def do_test(config):
            self.logger = debug_logger()
            test_auditor = FakeDatabaseAuditor(config, logger=self.logger)
            files = os.listdir(self.testdir)
            for f in files:
                path = os.path.join(self.testdir, f)
                test_auditor.audit(path)
            self.assertEqual(test_auditor.failures, 2)
            self.assertEqual(test_auditor.passes, 3)
            self.assertEqual(
                {'passes': 3,
                 'failures': 2},
                test_auditor.logger.statsd_client.get_increment_counts())

        do_test({})
        do_test({'vacuum_threshold_size': 0})
        do_test({'vacuum_threshold_percent': 0})
        do_test({'vacuum_threshold_percent': 0,
                 'vacuum_threshold_size': 0})
        # If nothing matches the percent, then no vacumming is triggered
        # percent in this testing harness is the file number x 10. So 30%
        # is the maximum freelist percentage that'll ever be returned.
        do_test({'vacuum_threshold_percent': 30})
        do_test({'vacuum_threshold_percent': 30,
                 'vacuum_threshold_size': 0})
        # likewise the maximum "size" is the number. So any size bigger >= 3
        # wont trigger a vacuum
        do_test({'vacuum_threshold_size': 3})
        do_test({'vacuum_threshold_percent': 30,
                 'vacuum_threshold_size': 3})

    def test_database_auditor_freelist_vacuum_configs(self):
        def mock_audit_loc_generator(*args, **kargs):
            files = os.listdir(self.testdir)
            for f in files:
                path = os.path.join(self.testdir, f)
                yield path, "", ""

        def do_test(config, expected_inc_stats):
            self.logger = debug_logger()
            with mock.patch('swift.common.db_auditor.audit_location_generator',
                            mock_audit_loc_generator):
                test_auditor = FakeDatabaseAuditor(config, logger=self.logger)
                test_auditor.run_once()
            self.assertEqual(test_auditor.failures, 2)
            self.assertEqual(test_auditor.passes, 3)
            self.assertEqual(
                expected_inc_stats,
                test_auditor.logger.statsd_client.get_increment_counts())
            warn_lines = self.logger.get_lines_for_level('warning')
            self.assertIn("Vacuum failed on ", warn_lines[0])
            self.assertIn("Vacuum failed!", warn_lines[0])

        # first let's do an freelist_size check. Numbers bigger then the
        # vacuum_size get vacuumed. We're just using the number in the
        # filename, and set it to 1 below, so only true{2,3}.db will attempt
        # to be vacuumed.
        conf = {'vacuum_threshold_size': 1}
        expected_inc_stats = {
            'passes': 3,
            'failures': 2,
            # free list size check was matched twice to trigger a vacuum
            'freelist_size': 2,
            'vacuum.successes': 1,
            # odd numbers fail vacuum in this test harness ie true3.db
            # just so we can test metrics.
            'vacuum.failures': 1}
        do_test(conf, expected_inc_stats)

        # Now a percent check. This test harness is just using the same file
        # numbers, but then x 10. So the max itemlist_percentage is 30%.
        # let's basically do the same check.
        conf = {'vacuum_threshold_percent': 10}
        expected_inc_stats = {
            'passes': 3,
            'failures': 2,
            # free list size check was matched twice to trigger a vacuum
            'freelist_percent': 2,
            'vacuum.successes': 1,
            # odd numbers fail vacuum in this test harness ie true3.db
            # just so we can test metrics.
            'vacuum.failures': 1}
        do_test(conf, expected_inc_stats)

        # But what's cool, we can also do a percent and a size. Because
        # percent is really a ratio of the pages to free pages.
        conf = {'vacuum_threshold_percent': 10,
                # now we should only get the *3.db files matched for size
                'vacuum_threshold_size': 2}
        expected_inc_stats = {
            'passes': 3,
            'failures': 2,
            # percent was matched twice like before
            'freelist_percent': 2,
            # size only matched with 1 db.
            'freelist_size': 1,
            # Yet we still only attempted to only call vacuum the 2 times.
            'vacuum.successes': 1,
            'vacuum.failures': 1}
        do_test(conf, expected_inc_stats)

    def test_generate_freepage_stats(self):
        def do_test(percent, size, exp_percent_stat, exp_size_stat):
            broker = mock.Mock()
            broker.get_freelist_percent.return_value = percent
            broker.get_freelist_size.return_value = size
            logger = debug_logger()
            test_auditor = FakeDatabaseAuditor({}, logger=logger)
            test_auditor._init_vacuum_stats()
            test_auditor.generate_freepage_stats(broker)
            self.assertIn(exp_percent_stat,
                          test_auditor.freelist_stats['percent'])
            self.assertIn(exp_size_stat, test_auditor.freelist_stats['size'])

        mb = 1024 * 1024
        size_tests = (
            (0, 'container.freelist.size.0-1MB'),
            (1 * mb, 'container.freelist.size.0-1MB'),
            (2 * mb - 1, 'container.freelist.size.0-1MB'),
            # 2MB+ if in the next bucket
            (2 * mb, 'container.freelist.size.2-3MB'),
            (4 * mb - 1, 'container.freelist.size.2-3MB'),
            # buckets get bigger because they're power of 2
            (4 * mb, 'container.freelist.size.4-7MB'),
            (8 * mb, 'container.freelist.size.8-15MB'),
            (16 * mb, 'container.freelist.size.16-31MB'),
            (32 * mb, 'container.freelist.size.32-63MB'),
            (64 * mb, 'container.freelist.size.64-127MB'),
            (128 * mb, 'container.freelist.size.128-255MB'),
            # and should go even higher
            (13370 * mb, 'container.freelist.size.8192-16383MB'),
        )
        percent_tests = (
            (0, 'container.freelist.percent.0-9'),
            (9, 'container.freelist.percent.0-9'),
            (10, 'container.freelist.percent.10-19'),
            (19, 'container.freelist.percent.10-19'),
            (20, 'container.freelist.percent.20-29'),
            (45, 'container.freelist.percent.40-49'),
            (59, 'container.freelist.percent.50-59'),
            (70, 'container.freelist.percent.70-79'),
            (75, 'container.freelist.percent.70-79'),
            (99, 'container.freelist.percent.90-99'),
            (100, 'container.freelist.percent.100-109'),
            (101, 'container.freelist.percent.100-109'),
        )
        for (size, size_stat), (percent, per_stat) \
                in zip(size_tests, percent_tests):
            do_test(percent, size, per_stat, size_stat)


if __name__ == '__main__':
    unittest.main()
