# Copyright (c) 2022 NVIDIA
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

import mock
import signal
import six
import subprocess
import unittest

from six.moves import StringIO
from swift.cli import reload


@mock.patch('sys.stderr', new_callable=StringIO)
@mock.patch('subprocess.check_output')
class TestValidateManagerPid(unittest.TestCase):
    def test_good(self, mock_check_output, mock_stderr):
        cmdline = (
            '/usr/local/bin/python3.9 '
            '/usr/local/bin/swift-proxy-server '
            '/etc/swift/proxy-server.conf '
            'some extra args'
        )
        mock_check_output.return_value = ('123  1  123  %s\n\n' % cmdline)
        self.assertEqual(reload.validate_manager_pid(123), (
            cmdline,
            'swift-proxy-server',
        ))
        kw = {} if six.PY2 else {'encoding': 'utf-8'}
        self.assertEqual(mock_check_output.mock_calls, [
            mock.call(['ps', '-p', '123', '--no-headers', '-o',
                       'sid,ppid,pid,cmd'], **kw),
        ])

    def test_ps_error(self, mock_check_output, mock_stderr):
        mock_check_output.side_effect = subprocess.CalledProcessError(
            137, 'ps')
        with self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(123)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        kw = {} if six.PY2 else {'encoding': 'utf-8'}
        self.assertEqual(mock_check_output.mock_calls, [
            mock.call(['ps', '-p', '123', '--no-headers', '-o',
                       'sid,ppid,pid,cmd'], **kw),
        ])
        self.assertEqual(mock_stderr.getvalue(),
                         'Failed to get process information for 123\n')

    def test_non_python(self, mock_check_output, mock_stderr):
        mock_check_output.return_value = ('123  34  56  /usr/bin/rsync\n\n')
        with self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(56)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        kw = {} if six.PY2 else {'encoding': 'utf-8'}
        self.assertEqual(mock_check_output.mock_calls, [
            mock.call(['ps', '-p', '56', '--no-headers', '-o',
                       'sid,ppid,pid,cmd'], **kw),
        ])
        self.assertEqual(mock_stderr.getvalue(),
                         "Non-swift process: '/usr/bin/rsync'\n")

    def test_non_swift(self, mock_check_output, mock_stderr):
        mock_check_output.return_value = ('123  34  56  /usr/bin/python\n\n')
        with self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(56)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        kw = {} if six.PY2 else {'encoding': 'utf-8'}
        self.assertEqual(mock_check_output.mock_calls, [
            mock.call(['ps', '-p', '56', '--no-headers', '-o',
                       'sid,ppid,pid,cmd'], **kw),
        ])
        self.assertEqual(mock_stderr.getvalue(),
                         "Non-swift process: '/usr/bin/python'\n")

    def test_worker(self, mock_check_output, mock_stderr):
        cmdline = (
            '/usr/bin/python3.9 '
            '/usr/bin/swift-proxy-server '
            '/etc/swift/proxy-server.conf'
        )
        mock_check_output.return_value = ('123  34  56  %s\n\n' % cmdline)
        with self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(56)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        kw = {} if six.PY2 else {'encoding': 'utf-8'}
        self.assertEqual(mock_check_output.mock_calls, [
            mock.call(['ps', '-p', '56', '--no-headers', '-o',
                       'sid,ppid,pid,cmd'], **kw),
        ])
        self.assertEqual(mock_stderr.getvalue(),
                         'Process appears to be a worker, not a manager. '
                         'Did you mean 123?\n')


class TestMain(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch('sys.stderr', new_callable=StringIO)
        self.mock_stderr = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch('subprocess.check_call')
        self.mock_check_call = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch.object(reload, 'validate_manager_pid')
        self.mock_validate = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch.object(reload, 'get_child_pids')
        self.mock_get_child_pids = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch('os.kill')
        self.mock_kill = patcher.start()
        self.addCleanup(patcher.stop)

    def test_good(self):
        self.mock_validate.return_value = ('cmdline', 'swift-proxy-server')
        self.mock_get_child_pids.side_effect = [
            {'worker1', 'worker2'},
            {'worker1', 'worker2', 'foster parent'},
            {'worker1', 'worker2', 'foster parent', 'new worker'},
            {'worker1', 'worker2', 'new worker'},
        ]
        self.assertIsNone(reload.main(['123']))
        self.assertEqual(self.mock_check_call.mock_calls, [
            mock.call(['cmdline', '--test-config']),
        ])
        self.assertEqual(self.mock_kill.mock_calls, [
            mock.call(123, signal.SIGUSR1),
        ])

    @mock.patch('time.time', side_effect=[1, 10, 100, 400])
    def test_timeout(self, mock_time):
        self.mock_validate.return_value = ('cmdline', 'swift-proxy-server')
        self.mock_get_child_pids.side_effect = [
            {'worker1', 'worker2'},
            {'worker1', 'worker2', 'foster parent'},
            {'worker1', 'worker2', 'foster parent', 'new worker'},
        ]
        with self.assertRaises(SystemExit) as caught:
            reload.main(['123'])
        self.assertEqual(caught.exception.args, (reload.EXIT_RELOAD_TIMEOUT,))
        self.assertEqual(self.mock_check_call.mock_calls, [
            mock.call(['cmdline', '--test-config']),
        ])
        self.assertEqual(self.mock_kill.mock_calls, [
            mock.call(123, signal.SIGUSR1),
        ])
        self.assertEqual(self.mock_stderr.getvalue(),
                         'Timed out reloading swift-proxy-server\n')

    def test_non_server(self):
        self.mock_validate.return_value = ('cmdline', 'swift-ring-builder')
        with self.assertRaises(SystemExit) as caught:
            reload.main(['123'])
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        self.assertEqual(self.mock_stderr.getvalue(),
                         'Process does not support config checks: '
                         'swift-ring-builder\n')
        self.assertEqual(self.mock_kill.mock_calls, [])

    def test_check_failed(self):
        self.mock_validate.return_value = ('cmdline', 'swift-object-server')
        self.mock_check_call.side_effect = subprocess.CalledProcessError(
            2, 'swift-object-server')
        with self.assertRaises(SystemExit) as caught:
            reload.main(['123'])
        self.assertEqual(caught.exception.args, (reload.EXIT_RELOAD_FAILED,))
        self.assertEqual(self.mock_check_call.mock_calls, [
            mock.call(['cmdline', '--test-config']),
        ])
        self.assertEqual(self.mock_kill.mock_calls, [])

    def test_needs_pid(self):
        with self.assertRaises(SystemExit) as caught:
            reload.main([])
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        msg = 'usage: \nSafely reload WSGI servers'
        self.assertEqual(self.mock_stderr.getvalue()[:len(msg)], msg)
        if six.PY2:
            msg = '\n: error: too few arguments\n'
        else:
            msg = '\n: error: the following arguments are required: pid\n'
        self.assertEqual(self.mock_stderr.getvalue()[-len(msg):], msg)
