# Copyright (c) 2024 NVIDIA
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
import unittest
from unittest import mock

import zlib

from swift.common.utils import checksum


# If you're curious about the 0xe3069283, see "check" at
# https://reveng.sourceforge.io/crc-catalogue/17plus.htm#crc.cat.crc-32-iscsi
class TestCRC32C(unittest.TestCase):
    def check_crc_func(self, impl):
        self.assertEqual(impl(b"123456789"), 0xe3069283)
        # Check that we can save/continue
        partial = impl(b"12345")
        self.assertEqual(impl(b"6789", partial), 0xe3069283)

    def test_ref(self):
        self.check_crc_func(checksum.crc32c_ref)
        # Check preferences -- choice of last resort
        if checksum.crc32c_isal is None and checksum.crc32c_kern is None:
            self.assertIs(checksum._select_crc32c_impl(), checksum.crc32c_ref)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern(self):
        self.check_crc_func(checksum.crc32c_kern)
        # Check preferences -- beats out reference, but not ISA-L
        if checksum.crc32c_isal is None:
            self.assertIs(checksum._select_crc32c_impl(), checksum.crc32c_kern)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern_socket_close_happy_path(self):
        mock_crc32c_socket = mock.MagicMock()
        mock_socket = mock.MagicMock()
        mock_socket.recv.return_value = b'1234'
        mock_crc32c_socket.accept.return_value = (mock_socket, None)
        with mock.patch('swift.common.utils.checksum.socket.socket',
                        return_value=mock_crc32c_socket):
            checksum.crc32c_kern(b'x')
        self.assertEqual([mock.call()],
                         mock_socket.close.call_args_list)
        self.assertEqual([mock.call()],
                         mock_crc32c_socket.close.call_args_list)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern_socket_close_after_bind_error(self):
        mock_crc32c_socket = mock.MagicMock()
        mock_crc32c_socket.bind.side_effect = OSError('boom')
        with mock.patch('swift.common.utils.checksum.socket.socket',
                        return_value=mock_crc32c_socket):
            with self.assertRaises(OSError) as cm:
                checksum.crc32c_kern(b'x')
        self.assertEqual('boom', str(cm.exception))
        self.assertEqual([mock.call()],
                         mock_crc32c_socket.close.call_args_list)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern_socket_close_after_setsockopt_error(self):
        mock_crc32c_socket = mock.MagicMock()
        mock_crc32c_socket.setsockopt.side_effect = OSError('boom')
        with mock.patch('swift.common.utils.checksum.socket.socket',
                        return_value=mock_crc32c_socket):
            with self.assertRaises(OSError) as cm:
                checksum.crc32c_kern(b'x')
        self.assertEqual('boom', str(cm.exception))
        self.assertEqual([mock.call()],
                         mock_crc32c_socket.close.call_args_list)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern_socket_close_after_accept_error(self):
        mock_crc32c_socket = mock.MagicMock()
        mock_crc32c_socket.accept.side_effect = OSError('boom')
        with mock.patch('swift.common.utils.checksum.socket.socket',
                        return_value=mock_crc32c_socket):
            with self.assertRaises(OSError) as cm:
                checksum.crc32c_kern(b'x')
        self.assertEqual('boom', str(cm.exception))
        self.assertEqual([mock.call()],
                         mock_crc32c_socket.close.call_args_list)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern_socket_after_sendall_error(self):
        mock_crc32c_socket = mock.MagicMock()
        mock_socket = mock.MagicMock()
        mock_socket.sendall.side_effect = OSError('boom')
        mock_crc32c_socket.accept.return_value = (mock_socket, None)
        with mock.patch('swift.common.utils.checksum.socket.socket',
                        return_value=mock_crc32c_socket):
            with self.assertRaises(OSError) as cm:
                checksum.crc32c_kern(b'x')
        self.assertEqual('boom', str(cm.exception))
        self.assertEqual([mock.call()],
                         mock_socket.close.call_args_list)
        self.assertEqual([mock.call()],
                         mock_crc32c_socket.close.call_args_list)

    @unittest.skipIf(checksum.crc32c_kern is None, 'No kernel CRC32C')
    def test_kern_socket_after_recv_error(self):
        mock_crc32c_socket = mock.MagicMock()
        mock_socket = mock.MagicMock()
        mock_socket.recv.side_effect = OSError('boom')
        mock_crc32c_socket.accept.return_value = (mock_socket, None)
        with mock.patch('swift.common.utils.checksum.socket.socket',
                        return_value=mock_crc32c_socket):
            with self.assertRaises(OSError) as cm:
                checksum.crc32c_kern(b'x')
        self.assertEqual('boom', str(cm.exception))
        self.assertEqual([mock.call()],
                         mock_socket.close.call_args_list)
        self.assertEqual([mock.call()],
                         mock_crc32c_socket.close.call_args_list)

    @unittest.skipIf(checksum.crc32c_isal is None, 'No ISA-L CRC32C')
    def test_isal(self):
        self.check_crc_func(checksum.crc32c_isal)
        # Check preferences -- ISA-L always wins
        self.assertIs(checksum._select_crc32c_impl(), checksum.crc32c_isal)

    def test_equivalence(self):
        data = os.urandom(64 * 1024)
        ref_crc = checksum.crc32c_ref(data)
        self.assertEqual(checksum.crc32c(data).crc, ref_crc)
        if checksum.crc32c_kern is not None:
            self.assertEqual(checksum.crc32c_kern(data), ref_crc)
        if checksum.crc32c_isal is not None:
            self.assertEqual(checksum.crc32c_isal(data), ref_crc)


class TestCRCHasher(unittest.TestCase):
    def test_base_crc_hasher(self):
        func = mock.MagicMock(return_value=0xbad1)
        hasher = checksum.CRCHasher('fake', func)
        self.assertEqual('fake', hasher.name)
        self.assertEqual(32, hasher.width)
        self.assertEqual(0, hasher.crc)
        self.assertEqual(b'\x00\x00\x00\x00', hasher.digest())
        self.assertEqual('00000000', hasher.hexdigest())

        hasher.update(b'123456789')
        self.assertEqual(0xbad1, hasher.crc)
        self.assertEqual(b'\x00\x00\xba\xd1', hasher.digest())
        self.assertEqual('0000bad1', hasher.hexdigest())

    def test_crc32_hasher(self):
        # See CRC-32/ISO-HDLC at
        # https://reveng.sourceforge.io/crc-catalogue/17plus.htm
        hasher = checksum.crc32()
        self.assertEqual('crc32', hasher.name)
        self.assertEqual(4, hasher.digest_size)
        self.assertEqual(zlib.crc32, hasher.crc_func)
        self.assertEqual(32, hasher.width)
        self.assertEqual(0, hasher.crc)
        self.assertEqual(b'\x00\x00\x00\x00', hasher.digest())
        self.assertEqual('00000000', hasher.hexdigest())

        hasher.update(b'123456789')
        self.assertEqual(0xcbf43926, hasher.crc)
        self.assertEqual(b'\xcb\xf4\x39\x26', hasher.digest())
        self.assertEqual('cbf43926', hasher.hexdigest())

    def test_crc32_hasher_contructed_with_data(self):
        hasher = checksum.crc32(b'123456789')
        self.assertEqual(zlib.crc32, hasher.crc_func)
        self.assertEqual(0xcbf43926, hasher.crc)
        self.assertEqual(b'\xcb\xf4\x39\x26', hasher.digest())
        self.assertEqual('cbf43926', hasher.hexdigest())

    def test_crc32_hasher_initial_value(self):
        hasher = checksum.crc32(initial_value=0xcbf43926)
        self.assertEqual(zlib.crc32, hasher.crc_func)
        self.assertEqual(0xcbf43926, hasher.crc)
        self.assertEqual(b'\xcb\xf4\x39\x26', hasher.digest())
        self.assertEqual('cbf43926', hasher.hexdigest())

    def test_crc32_hasher_copy(self):
        hasher = checksum.crc32(b'123456789')
        self.assertEqual(4, hasher.digest_size)
        self.assertEqual('cbf43926', hasher.hexdigest())
        hasher_copy = hasher.copy()
        self.assertEqual('crc32', hasher.name)
        self.assertEqual(zlib.crc32, hasher_copy.crc_func)
        self.assertEqual('cbf43926', hasher_copy.hexdigest())
        hasher_copy.update(b'foo')
        self.assertEqual('cbf43926', hasher.hexdigest())
        self.assertEqual('04e7e407', hasher_copy.hexdigest())
        hasher.update(b'bar')
        self.assertEqual('fe6b0d8c', hasher.hexdigest())
        self.assertEqual('04e7e407', hasher_copy.hexdigest())

    def test_crc32c_hasher(self):
        # See CRC-32/ISCSI at
        # https://reveng.sourceforge.io/crc-catalogue/17plus.htm
        hasher = checksum.crc32c()
        self.assertEqual('crc32c', hasher.name)
        self.assertEqual(32, hasher.width)
        self.assertEqual(0, hasher.crc)
        self.assertEqual(b'\x00\x00\x00\x00', hasher.digest())
        self.assertEqual('00000000', hasher.hexdigest())

        hasher.update(b'123456789')
        self.assertEqual(0xe3069283, hasher.crc)
        self.assertEqual(b'\xe3\x06\x92\x83', hasher.digest())
        self.assertEqual('e3069283', hasher.hexdigest())

    def test_crc32c_hasher_constructed_with_data(self):
        hasher = checksum.crc32c(b'123456789')
        self.assertEqual(0xe3069283, hasher.crc)
        self.assertEqual(b'\xe3\x06\x92\x83', hasher.digest())
        self.assertEqual('e3069283', hasher.hexdigest())

    def test_crc32c_hasher_initial_value(self):
        hasher = checksum.crc32c(initial_value=0xe3069283)
        self.assertEqual(0xe3069283, hasher.crc)
        self.assertEqual(b'\xe3\x06\x92\x83', hasher.digest())
        self.assertEqual('e3069283', hasher.hexdigest())

    def test_crc32c_hasher_copy(self):
        hasher = checksum.crc32c(b'123456789')
        self.assertEqual('e3069283', hasher.hexdigest())
        hasher_copy = hasher.copy()
        self.assertEqual('crc32c', hasher_copy.name)
        self.assertIs(hasher.crc_func, hasher_copy.crc_func)
        self.assertEqual('e3069283', hasher_copy.hexdigest())
        hasher_copy.update(b'foo')
        self.assertEqual('e3069283', hasher.hexdigest())
        self.assertEqual('6b2fc5b0', hasher_copy.hexdigest())
        hasher.update(b'bar')
        self.assertEqual('ae5c789c', hasher.hexdigest())
        self.assertEqual('6b2fc5b0', hasher_copy.hexdigest())

    def test_crc32c_hasher_impl(self):
        with mock.patch('swift.common.utils.checksum.crc32c_isal', None), \
                mock.patch('swift.common.utils.checksum.crc32c_kern', None):
            self.assertIs(checksum.crc32c_ref, checksum.crc32c().crc_func)

        with mock.patch('swift.common.utils.checksum.crc32c_isal', None), \
                mock.patch(
                    'swift.common.utils.checksum.crc32c_kern') as mock_kern:
            self.assertIs(mock_kern, checksum.crc32c().crc_func)

        with mock.patch(
                'swift.common.utils.checksum.crc32c_isal') as mock_isal, \
                mock.patch('swift.common.utils.checksum.crc32c_kern'):
            self.assertIs(mock_isal, checksum.crc32c().crc_func)
