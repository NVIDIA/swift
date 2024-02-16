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


class TestCRC64NVME(unittest.TestCase):
    def check_crc_func(self, impl):
        self.assertEqual(impl(b"123456789"), 0xae8b14860a799888)
        # Check that we can save/continue
        partial = impl(b"12345")
        self.assertEqual(impl(b"6789", partial), 0xae8b14860a799888)

    def test_ref(self):
        self.check_crc_func(checksum.crc64nvme_ref)
        if checksum.crc64nvme_isal is None:
            self.assertIs(checksum._select_crc64nvme_impl(),
                          checksum.crc64nvme_ref)

    @unittest.skipIf(checksum.crc64nvme_isal is None, 'No ISA-L CRC64NVME')
    def test_isal(self):
        self.check_crc_func(checksum.crc64nvme_isal)
        # Check preferences -- ISA-L always wins
        self.assertIs(checksum._select_crc64nvme_impl(),
                      checksum.crc64nvme_isal)

    def test_equivalence(self):
        data = os.urandom(64 * 1024)
        ref_crc = checksum.crc64nvme_ref(data)
        self.assertEqual(checksum.crc64nvme(data).crc, ref_crc)
        if checksum.crc64nvme_isal is not None:
            self.assertEqual(checksum.crc64nvme_isal(data), ref_crc)


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
            self.assertEqual(checksum.crc32c_ref,
                             checksum.crc32c().crc_func)

        with mock.patch('swift.common.utils.checksum.crc32c_isal', None), \
                mock.patch(
                    'swift.common.utils.checksum.crc32c_kern') as mock_kern:
            self.assertEqual(mock_kern, checksum.crc32c().crc_func)

        with mock.patch(
                'swift.common.utils.checksum.crc32c_isal') as mock_isal, \
                mock.patch('swift.common.utils.checksum.crc32c_kern'):
            self.assertEqual(mock_isal, checksum.crc32c().crc_func)

    def test_crc64nvme_hasher(self):
        # See CRC-64/NVME at
        # https://reveng.sourceforge.io/crc-catalogue/17plus.htm
        hasher = checksum.crc64nvme()
        self.assertEqual('crc64nvme', hasher.name)
        self.assertEqual(8, hasher.digest_size)
        self.assertEqual(64, hasher.width)
        self.assertEqual(0, hasher.crc)
        self.assertEqual(b'\x00\x00\x00\x00\x00\x00\x00\x00', hasher.digest())
        self.assertEqual('0000000000000000', hasher.hexdigest())

        hasher.update(b'123456789')
        self.assertEqual(0xae8b14860a799888, hasher.crc)
        self.assertEqual(b'\xae\x8b\x14\x86\x0a\x79\x98\x88', hasher.digest())
        self.assertEqual('ae8b14860a799888', hasher.hexdigest())

    def test_crc64nvme_hasher_constructed_with_data(self):
        hasher = checksum.crc64nvme(b'123456789')
        self.assertEqual(b'\xae\x8b\x14\x86\x0a\x79\x98\x88', hasher.digest())
        self.assertEqual('ae8b14860a799888', hasher.hexdigest())

    def test_crc64nvme_hasher_initial_value(self):
        hasher = checksum.crc64nvme(initial_value=0xae8b14860a799888)
        self.assertEqual(b'\xae\x8b\x14\x86\x0a\x79\x98\x88', hasher.digest())
        self.assertEqual('ae8b14860a799888', hasher.hexdigest())

    def test_crc64nvme_hasher_copy(self):
        hasher = checksum.crc64nvme(b'123456789')
        self.assertEqual('ae8b14860a799888', hasher.hexdigest())
        hasher_copy = hasher.copy()
        self.assertEqual('crc64nvme', hasher_copy.name)
        self.assertIs(hasher.crc_func, hasher_copy.crc_func)
        self.assertEqual('ae8b14860a799888', hasher_copy.hexdigest())
        hasher_copy.update(b'foo')
        self.assertEqual('ae8b14860a799888', hasher.hexdigest())
        self.assertEqual('673ece0d56523f46', hasher_copy.hexdigest())
        hasher.update(b'bar')
        self.assertEqual('0991d5edf1b0062e', hasher.hexdigest())
        self.assertEqual('673ece0d56523f46', hasher_copy.hexdigest())

    def test_crc64nvme_hasher_impl(self):
        with mock.patch('swift.common.utils.checksum.crc64nvme_isal', None):
            self.assertEqual(checksum.crc64nvme_ref,
                             checksum.crc64nvme().crc_func)

        with mock.patch(
                'swift.common.utils.checksum.crc64nvme_isal') as mock_isal:
            self.assertEqual(mock_isal, checksum.crc64nvme().crc_func)
