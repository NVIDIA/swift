# Copyright (c) 2010-2023 OpenStack Foundation
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

"""Tests for swift.common.utils.timestamp"""
import os
import random
import time
import unittest

from unittest import mock

from swift.common.utils import timestamp

from test.unit import mock_timestamp_randint, BaseUnitTestCase


class TestNormalTimestamp(unittest.TestCase):
    def _do_test_init(self, value):
        ts = timestamp.NormalTimestamp(value)
        self.assertEqual(1234567890.12346, float(ts))
        self.assertEqual(1234567890.12346, ts.timestamp)
        self.assertEqual(1234567890, int(ts))
        self.assertEqual(1234567891, ts.ceil())
        self.assertEqual('1234567890.12346', ts.normal)
        self.assertEqual('1234567890.12346', repr(ts))
        self.assertEqual(123456789012346, ts.raw)
        self.assertEqual('2009-02-13T23:31:30.123460', ts.isoformat)
        self.assertTrue(ts)

    def test_init(self):
        self._do_test_init(1234567890.12346)
        self._do_test_init('1234567890.12346')
        self._do_test_init(b'1234567890.12346')

    def test_init_from_timestamp(self):
        # A Timestamp with hex part cannot be cast to a NormalTimestamp
        ts = timestamp.Timestamp(1234567890.12346, offset=1)
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp(ts)
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp(ts.internal)

        # ...unless it has already been reduced to normal form
        self._do_test_init(ts.normal)
        self._do_test_init(ts.normalized())

    def test_now(self):
        now = time.time()
        with mock.patch('swift.common.utils.timestamp.time.time',
                        return_value=now):
            ts = timestamp.NormalTimestamp.now()
        exp = round(float((int(round(now / 1e-5)) * 1e-5)), 5)
        self.assertEqual(exp, float(ts))

    def test_check_bounds(self):
        ts = timestamp.NormalTimestamp(0)
        self.assertEqual(0.0, float(ts))
        self.assertEqual('0000000000.00000', ts.normal)

        ts = timestamp.NormalTimestamp(9999999999.99999)
        self.assertEqual(9999999999.99999, float(ts))
        self.assertEqual('9999999999.99999', ts.normal)

        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp(10000000000.00000)
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp('10000000000.00000')
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp(-0.00001)
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp('-0.00001')

    def test_init_bad_values(self):
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp('bad value')
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp('1234567890.12346_0000000000000000')
        with self.assertRaises(ValueError):
            timestamp.NormalTimestamp('1234567890.12346_0000000000000abc')

    def test_equality(self):
        ts = timestamp.NormalTimestamp(1234567890.12346)
        self.assertEqual(ts, timestamp.NormalTimestamp(1234567890.12346))
        self.assertEqual(ts, timestamp.NormalTimestamp('1234567890.12346'))
        self.assertEqual(
            ts, timestamp.NormalTimestamp(1234567890.12345, delta=1))
        self.assertEqual(ts, 1234567890.12346)
        self.assertEqual(ts, '1234567890.12346')

    def test_inequality(self):
        ts1 = timestamp.NormalTimestamp(1234567890.12346)
        self.assertNotEqual(ts1, timestamp.NormalTimestamp('1234567890.12345'))
        self.assertNotEqual(
            ts1, timestamp.NormalTimestamp(1234567890.12346, delta=1))

    def test_lt(self):
        ts1 = timestamp.NormalTimestamp(1234567890.12346)
        ts2 = timestamp.NormalTimestamp(1234567890.12347)
        self.assertLess(ts1, ts2)
        self.assertLess(ts1.internal, ts2)
        self.assertLess(ts1, ts2.internal)
        self.assertLess(ts1, 9999999999.00000)
        self.assertLess(ts1, '9999999999.00000')

    def test_gt(self):
        ts1 = timestamp.NormalTimestamp(1234567890.12346)
        ts2 = timestamp.NormalTimestamp(1234567890.12347)
        self.assertGreater(ts2, ts1)
        self.assertGreater(ts2.internal, ts1)
        self.assertGreater(ts2, ts1.internal)
        self.assertGreater(ts1, 0)
        self.assertGreater(ts1, -1)
        self.assertGreater(ts1, '0')
        self.assertGreater(ts1, '-1')
        self.assertGreater(ts1, '-123.456_0')
        self.assertGreater(ts1, None)
        self.assertGreater(timestamp.NormalTimestamp.zero(), None)

    def test_comparison_unsupported(self):
        ts = timestamp.NormalTimestamp.now()
        with self.assertRaises(TypeError) as cm:
            self.assertGreater(ts, True)
        self.assertEqual("'>' not supported between instances of "
                         "'NormalTimestamp' and 'bool'", str(cm.exception))

        with self.assertRaises(TypeError) as cm:
            self.assertGreater(ts, 'not a timestamp')
        self.assertEqual("'>' not supported between instances of "
                         "'NormalTimestamp' and 'str'", str(cm.exception))

    def test_from_isoformat(self):
        ts = timestamp.NormalTimestamp.from_isoformat(
            '2014-06-10T22:47:32.054580')
        self.assertIsInstance(ts, timestamp.NormalTimestamp)
        self.assertEqual(1402440452.05458, float(ts))
        self.assertEqual('2014-06-10T22:47:32.054580', ts.isoformat)

    def test_false(self):
        self.assertFalse(timestamp.NormalTimestamp(0))
        self.assertFalse(timestamp.NormalTimestamp('0'))
        self.assertFalse(timestamp.NormalTimestamp.zero())


class TestTimestamp(unittest.TestCase):
    """Tests for swift.common.utils.timestamp.Timestamp"""
    def test_zero(self):
        ts_zero = timestamp.Timestamp.zero()
        self.assertIsInstance(ts_zero, timestamp.Timestamp)
        self.assertEqual(0.0, float(ts_zero))
        self.assertEqual(0, ts_zero.offset)
        self.assertEqual(ts_zero.internal, '0000000000.00000')
        ts_other = timestamp.Timestamp(timestamp.Timestamp.zero())
        self.assertEqual(timestamp.Timestamp.zero(), ts_other)
        self.assertEqual(timestamp.Timestamp('0'), ts_zero)
        self.assertEqual(timestamp.Timestamp(0), ts_zero)
        self.assertNotEqual(timestamp.Timestamp(0, version=2), ts_zero)

    def test_init_invalid_version(self):
        with self.assertRaises(ValueError) as cm:
            timestamp.Timestamp(1234567890.12345, version=0)
        self.assertEqual('Invalid version', str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            timestamp.Timestamp(1234567890.12345, version=3)
        self.assertEqual('Invalid version', str(cm.exception))

    def test_init_version_not_allowed(self):
        with self.assertRaises(TypeError) as cm:
            timestamp.Timestamp('1234567890.12345_200000000a000001', version=1)
        self.assertEqual('version not allowed when parsing timestamp',
                         str(cm.exception))
        with self.assertRaises(TypeError) as cm:
            timestamp.Timestamp('1234567890.12345', version=2)
        self.assertEqual('version not allowed when parsing timestamp',
                         str(cm.exception))
        with self.assertRaises(TypeError) as cm:
            ts_orig = timestamp.Timestamp(1234567890.12345, version=2)
            timestamp.Timestamp(ts_orig, version=2)
        self.assertEqual('version not allowed when parsing timestamp',
                         str(cm.exception))

    def test_init_from_float_v1(self):
        ts = timestamp.Timestamp(1234567890.12345)
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12345', ts.internal)
        self.assertEqual('1234567890.12345', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)
        ts2 = timestamp.Timestamp(1234567890.12345, version=1)
        self.assertEqual(ts, ts2)

    @mock_timestamp_randint(0xa)
    def test_init_from_float_v2(self):
        ts = timestamp.Timestamp(1234567890.12345, version=2)
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12345_200000000a000000', ts.internal)
        self.assertEqual('1234567890.12345_200000000a000000', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

    def test_init_from_float_with_offset_v1(self):
        ts = timestamp.Timestamp(1234567890.12345, offset=1)
        self.assertEqual(1, ts.offset)
        self.assertEqual('1234567890.12345_0000000000000001', ts.internal)
        self.assertEqual('1234567890.12345_1', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

        ts = timestamp.Timestamp(1234567890.12345, offset=0xffffff)
        self.assertEqual(0xffffff, ts.offset)
        self.assertEqual('1234567890.12345_0000000000ffffff', ts.internal)
        self.assertEqual('1234567890.12345_ffffff', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

    @mock_timestamp_randint(0xa)
    def test_init_from_float_with_offset_v2(self):
        ts = timestamp.Timestamp(1234567890.12345, offset=1, version=2)
        self.assertEqual(1, ts.offset)
        self.assertEqual('1234567890.12345_200000000a000001', ts.internal)
        self.assertEqual('1234567890.12345_200000000a000001', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

        ts = timestamp.Timestamp(1234567890.12345, offset=0xffffff, version=2)
        self.assertEqual(0xffffff, ts.offset)
        self.assertEqual('1234567890.12345_200000000affffff', ts.internal)
        self.assertEqual('1234567890.12345_200000000affffff', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

    def test_init_from_float_with_delta(self):
        ts = timestamp.Timestamp(1234567890.12345, delta=1)
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12346', ts.internal)
        self.assertEqual('1234567890.12346', ts.short)
        self.assertEqual('1234567890.12346', ts.normal)

    @mock_timestamp_randint(0xa)
    def test_init_from_float_with_delta_v2(self):
        ts = timestamp.Timestamp(1234567890.12345, delta=1, version=2)
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12346_200000000a000000', ts.internal)
        self.assertEqual('1234567890.12346_200000000a000000', ts.short)
        self.assertEqual('1234567890.12346', ts.normal)

    def test_init_from_float_with_delta_v2_retains_jitter(self):
        ts1 = timestamp.Timestamp(1234567890.12345, version=2)
        ts2 = timestamp.Timestamp(ts1, delta=100000)
        self.assertEqual(float(ts2), float(ts1) + 1.0)
        self.assertEqual(ts2.hex_part, ts1.hex_part)

    def test_now_v1(self):
        now = time.time()
        with mock.patch('swift.common.utils.timestamp.time.time',
                        return_value=now):
            ts = timestamp.Timestamp.now(offset=0xfade)
        self.assertEqual(0xfade, ts.offset)
        self.assertEqual('%10.5f' % now, ts.normal)
        exp = round(float((int(round(now / 1e-5)) * 1e-5)), 5)
        self.assertEqual(exp, float(ts))
        float_part, hex_part = ts.internal.split('_')
        self.assertEqual(float_part, '%10.5f' % now)
        self.assertEqual(hex_part, '000000000000fade')

    def test_now_v2(self):
        actual = []
        now = time.time()

        for i in range(1000):
            with mock.patch('swift.common.utils.timestamp.time.time',
                            return_value=now):
                ts = timestamp.Timestamp.now(offset=0xfade, version=2)
            self.assertEqual(0xfade, ts.offset)
            self.assertEqual(ts.short, ts.internal)
            self.assertEqual('%10.5f' % now, ts.normal)
            exp = round(float((int(round(now / 1e-5)) * 1e-5)), 5)
            self.assertEqual(exp, float(ts))
            float_part, hex_part = ts.internal.split('_')
            self.assertEqual(float_part, '%10.5f' % now)
            self.assertGreaterEqual(hex_part, '200000000000fade')
            self.assertLess(hex_part, '300000000000fade')
            actual.append(hex_part)
        # if this fails then the same jitter was chosen more than once :(
        self.assertEqual(1000, len(set(actual)))

    def test_init_from_timestamp_v1(self):
        ts_orig = timestamp.Timestamp(1234567890.12345)
        ts = timestamp.Timestamp(ts_orig)
        self.assertEqual(ts_orig, ts)

        ts_orig = timestamp.Timestamp(1234567890.12345, offset=1)
        ts = timestamp.Timestamp(ts_orig)
        self.assertEqual(ts_orig, ts)

        # offset accumulates
        ts = timestamp.Timestamp(ts_orig, offset=2)
        self.assertEqual(3, ts.offset)
        self.assertEqual(float(ts_orig), float(ts))
        self.assertEqual(ts_orig.internal[:-1], ts.internal[:-1])

    def test_init_from_timestamp_v2(self):
        ts_orig = timestamp.Timestamp(1234567890.12345, version=2)
        ts = timestamp.Timestamp(ts_orig)
        self.assertEqual(ts_orig, ts)

        ts_orig = timestamp.Timestamp(1234567890.12345, offset=1)
        ts = timestamp.Timestamp(ts_orig)
        self.assertEqual(ts_orig, ts)

        # offset accumulates
        ts = timestamp.Timestamp(ts_orig, offset=2)
        self.assertEqual(3, ts.offset)
        self.assertEqual(float(ts_orig), float(ts))
        self.assertEqual(ts_orig.internal[:-1], ts.internal[:-1])

    def test_init_from_normal_timestamp(self):
        ts_orig = timestamp.NormalTimestamp(1234567890.12345)
        ts = timestamp.Timestamp(ts_orig)
        self.assertEqual('1234567890.12345', ts.internal)
        self.assertEqual(float(ts_orig), float(ts))
        self.assertEqual(ts_orig, ts)

        # offset added
        ts = timestamp.Timestamp(ts_orig, offset=1)
        self.assertEqual(1, ts.offset)
        self.assertEqual('1234567890.12345_0000000000000001', ts.internal)
        self.assertEqual(float(ts_orig), float(ts))
        self.assertNotEqual(ts_orig, ts)

    def test_init_legacy_from_str_v1(self):
        ts = timestamp.Timestamp('1234567890.12345')
        self.assertEqual('1234567890.12345', ts.internal)

        ts = timestamp.Timestamp('1234567890.12345_0000000000000abc')
        self.assertEqual('1234567890.12345_0000000000000abc', ts.internal)

        ts = timestamp.Timestamp('1234567890.12345_1000000000000abc')
        self.assertEqual('1234567890.12345_1000000000000abc', ts.internal)

        ts = timestamp.Timestamp('1234567890.12345_1fffffffffffffff')
        self.assertEqual('1234567890.12345_1fffffffffffffff', ts.internal)

    def test_init_from_str_v2(self):
        ts = timestamp.Timestamp('1234567890.12345')
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12345', ts.internal)

        ts = timestamp.Timestamp('1234567890.12345_2fffffffff000000')
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12345_2fffffffff000000', ts.internal)

        ts = timestamp.Timestamp('1234567890.12345_2fffffffff000001')
        self.assertEqual(1, ts.offset)
        self.assertEqual('1234567890.12345_2fffffffff000001', ts.internal)

        ts = timestamp.Timestamp('1234567890.12345_2fffffffffffffff')
        self.assertEqual(0xffffff, ts.offset)
        self.assertEqual('1234567890.12345_2fffffffffffffff', ts.internal)

    def test_init_from_str_v1_with_offset_arg(self):
        ts = timestamp.Timestamp('1234567890.12345', offset=1)
        self.assertEqual(1, ts.offset)
        self.assertEqual('1234567890.12345_0000000000000001', ts.internal)
        self.assertEqual('1234567890.12345_1', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

        ts = timestamp.Timestamp('1234567890.12345_1000000000000000',
                                 offset=0xffffff)
        self.assertEqual(0x1000000000ffffff, ts.offset)
        self.assertEqual('1234567890.12345_1000000000ffffff', ts.internal)
        self.assertEqual('1234567890.12345_1000000000ffffff', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

    def test_init_from_str_v2_with_offset_arg(self):
        ts = timestamp.Timestamp('1234567890.12345_2fffffffff000000', offset=1)
        self.assertEqual(1, ts.offset)
        self.assertEqual('1234567890.12345_2fffffffff000001', ts.internal)
        self.assertEqual('1234567890.12345_2fffffffff000001', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

        ts = timestamp.Timestamp('1234567890.12345_2fffffffff100000',
                                 offset=0xfffff)
        self.assertEqual(0x1fffff, ts.offset)
        self.assertEqual('1234567890.12345_2fffffffff1fffff', ts.internal)
        self.assertEqual('1234567890.12345_2fffffffff1fffff', ts.short)
        self.assertEqual('1234567890.12345', ts.normal)

    def test_init_from_str_v1_with_delta(self):
        ts = timestamp.Timestamp('1234567890.12345', delta=1)
        self.assertEqual(0, ts.offset)
        self.assertEqual('1234567890.12346', ts.internal)
        self.assertEqual('1234567890.12346', ts.short)
        self.assertEqual('1234567890.12346', ts.normal)

    def test_init_from_str_v2_with_delta(self):
        ts = timestamp.Timestamp('1234567890.12345_2123456789abcdef', delta=1)
        self.assertEqual(0xabcdef, ts.offset)
        self.assertEqual('1234567890.12346_2123456789abcdef', ts.internal)
        self.assertEqual('1234567890.12346_2123456789abcdef', ts.short)
        self.assertEqual('1234567890.12346', ts.normal)

    def test_init_future_version_from_str(self):
        ts = timestamp.Timestamp('1234567890.12345_3fffffffff000000')
        self.assertEqual('1234567890.12345_3fffffffff000000', ts.internal)
        self.assertEqual('1234567890.12345', ts.normal)
        self.assertGreater(ts, timestamp.Timestamp(1234567890.12345))
        with self.assertRaises(AttributeError) as cm:
            _ = ts.offset
        self.assertEqual('Cannot access offset in an unrecognised hex_part '
                         'encoding', str(cm.exception))

    def test_init_future_version_from_str_with_offset_is_error(self):
        with self.assertRaises(ValueError) as cm:
            timestamp.Timestamp('1234567890.12345_3fffffffff000000',
                                offset=1)
        self.assertEqual('Cannot modify offset in an unrecognised hex part '
                         'encoding', str(cm.exception))

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            timestamp.Timestamp(time.time(), offset=-1)
        with self.assertRaises(ValueError):
            timestamp.Timestamp('123.456_78_90')
        with self.assertRaises(ValueError):
            timestamp.Timestamp('')
        with self.assertRaises(ValueError):
            timestamp.Timestamp(None)
        with self.assertRaises(ValueError):
            timestamp.Timestamp('123.456_1234567890abcdef0')

    def test_invalid_string_conversion(self):
        t = timestamp.Timestamp.now()
        self.assertRaises(TypeError, str, t)

    def test_offset_property(self):
        ts = timestamp.Timestamp.now(version=2)
        internal = ts.internal
        self.assertEqual(0, ts.offset)
        ts = timestamp.Timestamp(ts, offset=1)
        self.assertEqual(1, ts.offset)
        self.assertEqual(internal[:-1] + '1', ts.internal)
        ts = timestamp.Timestamp(ts, offset=0xfffffe)
        self.assertEqual(0xffffff, ts.offset)
        self.assertEqual(internal[:-6] + 'ffffff', ts.internal)

        with self.assertRaises(AttributeError):
            ts.offset = 0x000001

    def test_offset_limit_v1(self):
        t_str = '1417462430.78693'
        # can't have a offset above MAX_OFFSET
        with self.assertRaises(ValueError):
            timestamp.Timestamp(t_str, offset=2 * (16 ** 16))
        # exactly max offset is fine
        ts = timestamp.Timestamp(t_str, offset=2 * (16 ** 15) - 1)
        self.assertEqual('1417462430.78693_1fffffffffffffff', ts.internal)
        # but you can't offset it further
        with self.assertRaises(ValueError) as cm:
            timestamp.Timestamp(ts.internal, offset=1)
        self.assertEqual(
            'offset must be less than or equal to 2305843009213693951',
            str(cm.exception))
        # unless you start below it
        ts = timestamp.Timestamp(t_str, offset=2 * (16 ** 15) - 2)
        self.assertEqual(timestamp.Timestamp(ts.internal, offset=1),
                         '1417462430.78693_1fffffffffffffff')

    @mock_timestamp_randint(0xa)
    def test_offset_limit_v2(self):
        t = 1417462430.78693
        # can't have a offset above MAX_OFFSET
        with self.assertRaises(ValueError):
            timestamp.Timestamp(t, offset=16 ** 6, version=2)
        # exactly max offset is fine
        ts = timestamp.Timestamp(t, offset=(16 ** 6) - 1, version=2)
        self.assertEqual('1417462430.78693_200000000affffff', ts.internal)
        # but you can't offset it further
        with self.assertRaises(ValueError) as cm:
            timestamp.Timestamp(ts.internal, offset=1)
        self.assertEqual('offset must be less than or equal to 16777215',
                         str(cm.exception))
        # unless you start below it
        ts = timestamp.Timestamp(t, offset=(16 ** 6) - 2, version=2)
        self.assertEqual('1417462430.78693_200000000affffff',
                         timestamp.Timestamp(ts.internal, offset=1))

    @mock_timestamp_randint(0xa)
    def test_increment_offset(self):
        ts = timestamp.Timestamp.now(version=2)
        self.assertEqual(0x200000000a000000, ts.hex_part)
        self.assertEqual(0, ts.offset)

        with self.assertRaises(ValueError):
            ts.increment_offset(-1)

        ts.increment_offset(0)
        self.assertEqual(0x200000000a000000, ts.hex_part)
        self.assertEqual(0, ts.offset)

        ts.increment_offset(1)
        self.assertEqual(0x200000000a000001, ts.hex_part)
        self.assertEqual(1, ts.offset)

        ts.increment_offset(0xfffffe)
        self.assertEqual(0x200000000affffff, ts.hex_part)
        self.assertEqual(0xffffff, ts.offset)

        with self.assertRaises(ValueError):
            ts.increment_offset(1)

    def test_normal_format_v2_has_no_hex_part(self):
        # there is only the float part in a normal representation
        expected = '1402436408.91203'
        test_values = (
            timestamp.Timestamp('1402436408.912030000_2000000000000'),
            timestamp.Timestamp('1402436408.912030000_2000000000001'),
            timestamp.Timestamp(1402436408.91203, version=2),
            timestamp.Timestamp(1402436408.912029, version=2),
            timestamp.Timestamp(1402436408.9120300000000000, version=2),
            timestamp.Timestamp(1402436408.91202999999999999, version=2),
            timestamp.Timestamp(timestamp.Timestamp(
                1402436408.91203, version=2)),
            timestamp.Timestamp(timestamp.Timestamp(
                1402436408.91203, offset=0, version=2)),
            timestamp.Timestamp(timestamp.Timestamp(
                1402436408.912029, version=2)),
            timestamp.Timestamp(timestamp.Timestamp(
                1402436408.912029, offset=0, version=2)),
            timestamp.Timestamp(timestamp.Timestamp(
                '1402436408.91203_2000000001000000', offset=1)),
            timestamp.Timestamp(timestamp.Timestamp(
                '1402436408.91203_2000000001000001', offset=1)),
        )
        for ts in test_values:
            with self.subTest(ts=ts):
                self.assertEqual(ts.normal, expected)
                # timestamp instance IS NOT equal float or stringified float
                self.assertNotEqual(ts, expected)
                self.assertNotEqual(ts, float(expected))
                self.assertNotEqual(
                    ts, timestamp.normalize_timestamp(expected))

    def test_normal_format_v1_has_no_offset(self):
        expected = '1402436408.91203'
        test_values = (
            '1402436408.91203',
            '1402436408.91203_00000000',
            '1402436408.912030000',
            '1402436408.912030000_0000000000000',
            '000001402436408.912030000',
            '000001402436408.912030000_0000000000',
            timestamp.Timestamp('1402436408.91203'),
            timestamp.Timestamp('1402436408.91203', offset=0),
            timestamp.Timestamp('1402436408.91203_00000000'),
            timestamp.Timestamp('1402436408.91203_00000000', offset=0),
        )
        for value in test_values:
            with self.subTest(value=value):
                ts = timestamp.Timestamp(value)
                # timestamp instance can also compare to string or float
                self.assertEqual(ts, expected)
                self.assertEqual(ts, float(expected))
                self.assertEqual(ts, timestamp.normalize_timestamp(expected))

    def test_isoformat(self):
        expected = '2014-06-10T22:47:32.054580'
        test_values = (
            '1402440452.05458',
            '1402440452.054579',
            '1402440452.05458_00000000',
            '1402440452.054579_00000000',
            '1402440452.054580000',
            '1402440452.054579999',
            '1402440452.054580000_0000000000000',
            '1402440452.054579999_0000ff00',
            '000001402440452.054580000',
            '000001402440452.0545799',
            '000001402440452.054580000_0000000000',
            '000001402440452.054579999999_00000fffff',
            1402440452.05458,
            1402440452.054579,
            1402440452.0545800000000000,
            1402440452.054579999,
            timestamp.Timestamp(1402440452.05458),
            timestamp.Timestamp(1402440452.0545799),
            timestamp.Timestamp(1402440452.05458, offset=0),
            timestamp.Timestamp(1402440452.05457999999, offset=0),
            timestamp.Timestamp(1402440452.05458, offset=100),
            timestamp.Timestamp(1402440452.054579, offset=100),
            timestamp.Timestamp('1402440452.05458'),
            timestamp.Timestamp('1402440452.054579999'),
            timestamp.Timestamp('1402440452.05458', offset=0),
            timestamp.Timestamp('1402440452.054579', offset=0),
            timestamp.Timestamp('1402440452.05458', offset=300),
            timestamp.Timestamp('1402440452.05457999', offset=300),
            timestamp.Timestamp('1402440452.05458_00000000'),
            timestamp.Timestamp('1402440452.05457999_00000000'),
            timestamp.Timestamp('1402440452.05458_00000000', offset=0),
            timestamp.Timestamp('1402440452.05457999_00000aaa', offset=0),
            timestamp.Timestamp('1402440452.05458_00000000', offset=400),
            timestamp.Timestamp('1402440452.054579_0a', offset=400),
        )
        for value in test_values:
            self.assertEqual(timestamp.Timestamp(value).isoformat, expected)
        expected = '1970-01-01T00:00:00.000000'
        test_values = (
            '0',
            '0000000000.00000',
            '0000000000.00000_ffffffffffff',
            0,
            0.0,
        )
        for value in test_values:
            self.assertEqual(timestamp.Timestamp(value).isoformat, expected)

    def test_from_isoformat(self):
        ts = timestamp.Timestamp.from_isoformat('2014-06-10T22:47:32.054580')
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertEqual(1402440452.05458, float(ts))
        self.assertEqual('1402440452.05458', ts.internal)
        self.assertEqual('2014-06-10T22:47:32.054580', ts.isoformat)

        ts = timestamp.Timestamp.from_isoformat('1970-01-01T00:00:00.000000')
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertEqual(0.0, float(ts))
        self.assertEqual('1970-01-01T00:00:00.000000', ts.isoformat)

        ts = timestamp.Timestamp('1402440452.05458')
        self.assertIsInstance(ts, timestamp.Timestamp)
        roundtrip_ts = timestamp.Timestamp.from_isoformat(ts.isoformat)
        self.assertEqual(ts.normal, roundtrip_ts.normal)
        self.assertEqual(ts, roundtrip_ts)

    def test_to_from_isoformat_v2(self):
        ts = timestamp.Timestamp.now(version=2)
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertNotEqual(0, ts.hex_part)
        roundtrip_ts = timestamp.Timestamp.from_isoformat(ts.isoformat)
        self.assertEqual(ts.normal, roundtrip_ts.normal)
        # the hex_part cannot be preserved!
        self.assertNotEqual(ts, roundtrip_ts)

    def test_ceil_v1(self):
        self.assertEqual(0.0, timestamp.Timestamp(0).ceil())
        self.assertEqual(1.0, timestamp.Timestamp(0.00001).ceil())
        self.assertEqual(0.0, timestamp.Timestamp(0.000001).ceil())
        self.assertEqual(1.0, timestamp.Timestamp(0.000006).ceil())
        self.assertEqual(12345678.0, timestamp.Timestamp(12345678.0).ceil())
        self.assertEqual(12345678.0,
                         timestamp.Timestamp(12345678.000001).ceil())
        self.assertEqual(12345679.0,
                         timestamp.Timestamp(12345678.000006).ceil())

    def test_ceil_v2(self):
        self.assertEqual(0.0, timestamp.Timestamp(0, version=2).ceil())
        self.assertEqual(1.0, timestamp.Timestamp(0.00001, version=2).ceil())
        self.assertEqual(0.0, timestamp.Timestamp(0.000001, version=2).ceil())
        self.assertEqual(1.0, timestamp.Timestamp(0.000006, version=2).ceil())
        self.assertEqual(
            12345678.0, timestamp.Timestamp(12345678.0, version=2).ceil())
        self.assertEqual(
            12345678.0, timestamp.Timestamp(12345678.000001, version=2).ceil())
        self.assertEqual(
            12345679.0, timestamp.Timestamp(12345678.000006, version=2).ceil())

    def test_equality_v1(self):
        ts = timestamp.Timestamp(1234567890.12346)
        self.assertEqual(ts, timestamp.Timestamp(ts))
        self.assertEqual(ts, timestamp.Timestamp(ts).internal)
        self.assertEqual(ts, timestamp.Timestamp('1234567890.12346'))
        self.assertEqual(ts, 1234567890.12346)
        self.assertEqual(ts, '1234567890.12346')

        ts = timestamp.Timestamp(1234567890.12346, offset=1)
        self.assertEqual(ts, timestamp.Timestamp(ts))
        self.assertEqual(ts, timestamp.Timestamp(ts).internal)
        self.assertEqual(
            ts, timestamp.Timestamp('1234567890.12346_0000000000000001'))
        self.assertEqual(ts, timestamp.Timestamp('1234567890.12346', offset=1))

    @mock_timestamp_randint(0xa)
    def test_equality_v2(self):
        ts = timestamp.Timestamp(1234567890.12346, version=2)
        self.assertEqual(ts, timestamp.Timestamp(ts))
        self.assertEqual(ts, timestamp.Timestamp(ts).internal)
        self.assertEqual(
            ts, timestamp.Timestamp('1234567890.12346_200000000a000000'))
        self.assertNotEqual(ts, 1234567890.12346)
        self.assertNotEqual(ts, '1234567890.12346')

        ts = timestamp.Timestamp(1234567890.12346, offset=1, version=2)
        self.assertEqual(ts, timestamp.Timestamp(ts))
        self.assertEqual(ts, timestamp.Timestamp(ts).internal)
        self.assertEqual(
            ts, timestamp.Timestamp('1234567890.12346_200000000a000001'))
        self.assertNotEqual(
            ts, timestamp.Timestamp('1234567890.12346', offset=1))

    def test_inequality_v2(self):
        ts = timestamp.Timestamp(1234567890.12346, version=2)
        self.assertNotEqual(
            ts, timestamp.Timestamp(1234567890.12346, version=2))
        self.assertNotEqual(ts, timestamp.Timestamp('1234567890.12346'))
        self.assertNotEqual(ts, 1234567890.12346)
        self.assertNotEqual(ts, '1234567890.12346')

    def test_inequality_v1(self):
        ts = '1402436408.91203_0000000000000001'
        test_values = (
            timestamp.Timestamp('1402436408.91203_0000000000000002'),
            timestamp.Timestamp('1402436408.91203'),
            timestamp.Timestamp(1402436408.91203),
            timestamp.Timestamp(1402436408.91204),
            timestamp.Timestamp(1402436408.91203, offset=0),
            timestamp.Timestamp(1402436408.91203, offset=2),
        )
        for value in test_values:
            self.assertTrue(value != ts)

        self.assertIs(True, timestamp.Timestamp(ts) == ts)  # sanity
        self.assertIs(False,
                      timestamp.Timestamp(ts) != timestamp.Timestamp(ts))
        self.assertIs(False, timestamp.Timestamp(ts) != ts)
        self.assertIs(False, timestamp.Timestamp(ts) is None)
        self.assertIs(True, timestamp.Timestamp(ts) is not None)

    def test_no_force_internal_no_offset_v1(self):
        """Test that internal is the same as normal with no offset"""
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=False):
            self.assertEqual(timestamp.Timestamp('0').internal,
                             '0000000000.00000')
            self.assertEqual(timestamp.Timestamp('1402437380.58186').internal,
                             '1402437380.58186')
            self.assertEqual(timestamp.Timestamp('1402437380.581859').internal,
                             '1402437380.58186')

    def test_no_force_internal_with_offset_v1(self):
        """Test that internal always includes the offset if significant"""
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=False):
            self.assertEqual(timestamp.Timestamp('0', offset=1).internal,
                             '0000000000.00000_0000000000000001')
            self.assertEqual(
                timestamp.Timestamp('1402437380.58186', offset=16).internal,
                '1402437380.58186_0000000000000010')
            self.assertEqual(
                timestamp.Timestamp('1402437380.581859', offset=240).internal,
                '1402437380.58186_00000000000000f0')
            self.assertEqual(
                timestamp.Timestamp('1402437380.581859_00000001',
                                    offset=240).internal,
                '1402437380.58186_00000000000000f1')

    def test_force_internal_v1(self):
        """Test that internal always includes the offset if forced"""
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=True):
            self.assertEqual(timestamp.Timestamp('0').internal,
                             '0000000000.00000_0000000000000000')
            self.assertEqual(timestamp.Timestamp('1402437380.58186').internal,
                             '1402437380.58186_0000000000000000')
            self.assertEqual(timestamp.Timestamp('1402437380.581859').internal,
                             '1402437380.58186_0000000000000000')
            self.assertEqual(timestamp.Timestamp('0', offset=1).internal,
                             '0000000000.00000_0000000000000001')
            self.assertEqual(
                timestamp.Timestamp('1402437380.58186', offset=16).internal,
                '1402437380.58186_0000000000000010')
            self.assertEqual(
                timestamp.Timestamp('1402437380.581859', offset=16).internal,
                '1402437380.58186_0000000000000010')

    @mock_timestamp_randint(0xa)
    def test_internal_format_no_offset_v2(self):
        expected = '1402436408.91203_200000000a000000'
        test_values = (
            timestamp.Timestamp('1402436408.91203_200000000a000000'),
            timestamp.Timestamp('000001402436408.912030000_200000000a000000'),
            timestamp.Timestamp(1402436408.91203, version=2),
            timestamp.Timestamp(1402436408.9120300000000000, version=2),
            timestamp.Timestamp(1402436408.912029, version=2),
            timestamp.Timestamp(1402436408.912029999999999999, version=2),
            timestamp.Timestamp(timestamp.Timestamp(1402436408.91203,
                                                    version=2)),
            timestamp.Timestamp(timestamp.Timestamp(1402436408.91203,
                                                    offset=0, version=2)),
            timestamp.Timestamp(timestamp.Timestamp(1402436408.912029,
                                                    version=2)),
            timestamp.Timestamp(1402436408.91202999999999999, offset=0,
                                version=2),
            timestamp.Timestamp(
                timestamp.Timestamp('1402436408.91203_200000000a000000')),
            timestamp.Timestamp(
                timestamp.Timestamp('1402436408.91203_200000000a000000',
                                    offset=0))
        )
        for value in test_values:
            with self.subTest(value=value):
                # timestamp instance is always equivalent
                self.assertEqual(expected, value)
                self.assertEqual(expected, value.internal)

    def test_internal_format_no_offset_v1(self):
        expected = '1402436408.91203_0000000000000000'
        test_values = (
            '1402436408.91203',
            '1402436408.91203_00000000',
            '1402436408.912030000',
            '1402436408.912030000_0000000000000',
            '000001402436408.912030000',
            '000001402436408.912030000_0000000000',
            timestamp.Timestamp('1402436408.91203'),
            timestamp.Timestamp('1402436408.91203', offset=0),
            timestamp.Timestamp('1402436408.912029'),
            timestamp.Timestamp('1402436408.912029', offset=0),
            timestamp.Timestamp('1402436408.912029999999999'),
            timestamp.Timestamp('1402436408.912029999999999', offset=0),
        )
        for value in test_values:
            with self.subTest(value=value):
                # timestamp instance is always equivalent
                self.assertEqual(timestamp.Timestamp(value), expected)
                if timestamp.FORCE_INTERNAL:
                    # the FORCE_INTERNAL flag makes the internal format always
                    # include the offset portion of the timestamp even when
                    # it's not significant and would be bad during upgrades
                    self.assertEqual(timestamp.Timestamp(value).internal,
                                     expected)
                else:
                    # unless we FORCE_INTERNAL, when there's no offset the
                    # internal format is equivalent to the normalized format
                    expected = '1402436408.91203'
                    self.assertEqual(timestamp.Timestamp(value).internal,
                                     expected)

    @mock_timestamp_randint(0xa)
    def test_internal_format_with_offset_v2(self):
        def do_test(ts):
            expected = '1402436408.91203_200000000a0000f0'
            self.assertEqual(expected, ts.internal)
            # can compare with offset if the string is internalized
            self.assertEqual(expected, ts)
            # if comparison value only includes the normalized portion and
            # the timestamp includes an offset, it is considered greater
            normal = timestamp.Timestamp(expected).normal
            self.assertGreater(ts, normal)
            self.assertGreater(ts, float(normal))

        do_test(
            timestamp.Timestamp('000001402436408.912030000_200000000a0000f0'))
        do_test(
            timestamp.Timestamp('000001402436408.9120299999_200000000a0000f0'))
        do_test(timestamp.Timestamp(1402436408.91203, offset=240, version=2))
        do_test(timestamp.Timestamp(1402436408.912029, offset=240, version=2))

    def test_internal_format_with_offset_v1(self):
        expected = '1402436408.91203_00000000000000f0'
        test_values = (
            '1402436408.91203_000000f0',
            u'1402436408.91203_000000f0',
            b'1402436408.91203_000000f0',
            '1402436408.912030000_0000000000f0',
            '1402436408.912029_000000f0',
            '1402436408.91202999999_0000000000f0',
            '000001402436408.912030000_000000000f0',
            '000001402436408.9120299999_000000000f0',
            timestamp.Timestamp('1402436408.91203', offset=240),
            timestamp.Timestamp('1402436408.91203_00000000', offset=240),
            timestamp.Timestamp('1402436408.91203_0000000f', offset=225),
            timestamp.Timestamp('1402436408.9120299999', offset=240),
            timestamp.Timestamp('1402436408.9120299999_00000000', offset=240),
            timestamp.Timestamp('1402436408.9120299999_00000010', offset=224),
        )
        for value in test_values:
            with self.subTest(value=value):
                ts = timestamp.Timestamp(value)
                self.assertEqual(ts.internal, expected)
                # can compare with offset if the string is internalized
                self.assertEqual(ts, expected)
                # if comparison value only includes the normalized portion and
                # the timestamp includes an offset, it is considered greater
                normal = timestamp.Timestamp(expected).normal
                self.assertTrue(ts > normal,
                                '%r is not bigger than %r given %r' % (
                                    ts, normal, value))
                self.assertTrue(ts > float(normal),
                                '%r is not bigger than %f given %r' % (
                                    ts, float(normal), value))

    @mock_timestamp_randint(0xa)
    def test_short_format_v2(self):
        # with jitter there is no abbreviation for short format
        expected = '1402436408.91203_200000000a0000f0'
        ts = timestamp.Timestamp(1402436408.91203, 0xf0, version=2)
        self.assertEqual(expected, ts.short)

        expected = '1402436408.91203_200000000a000000'
        ts = timestamp.Timestamp(1402436408.91203, version=2)
        self.assertEqual(expected, ts.short)

    def test_short_format_v1(self):
        expected = '1402436408.91203_f0'
        ts = timestamp.Timestamp('1402436408.91203', 0xf0)
        self.assertEqual(expected, ts.short)
        # short format is parsed as v1
        ts = timestamp.Timestamp('1402436408.91203_f0')
        self.assertEqual(expected, ts.short)
        # sanity check...
        self.assertEqual('1402436408.91203_00000000000000f0', ts.internal)

        # no offset
        expected = '1402436408.91203'
        ts = timestamp.Timestamp('1402436408.91203')
        self.assertEqual(expected, ts.short)

    def test_raw(self):
        expected = 140243640891203
        ts = timestamp.Timestamp(1402436408.91203, version=2)
        self.assertEqual(expected, ts.raw)
        self.assertEqual('1402436408.91203', ts.normal)

        # 'raw' does not include offset
        ts = timestamp.Timestamp(1402436408.91203, 0xf0, version=2)
        self.assertEqual(expected, ts.raw)

        expected = 175507756652338
        ts = timestamp.Timestamp(1755077566.523385, version=2)
        self.assertEqual(expected, ts.raw)
        self.assertEqual('1755077566.52338', ts.normal)

    def test_delta(self):
        def _assertWithinBounds(expected, timestamp):
            tolerance = 0.00001
            minimum = expected - tolerance
            maximum = expected + tolerance
            self.assertTrue(float(timestamp) > minimum)
            self.assertTrue(float(timestamp) < maximum)

        ts = timestamp.Timestamp(1402436408.91203, delta=100)
        _assertWithinBounds(1402436408.91303, ts)
        self.assertEqual(140243640891303, ts.raw)

        ts = timestamp.Timestamp(1402436408.91203, delta=-100)
        _assertWithinBounds(1402436408.91103, ts)
        self.assertEqual(140243640891103, ts.raw)

        ts = timestamp.Timestamp(1402436408.91203, delta=0)
        _assertWithinBounds(1402436408.91203, ts)
        self.assertEqual(140243640891203, ts.raw)

        # delta is independent of offset
        ts = timestamp.Timestamp(1402436408.91203, offset=42, delta=100)
        self.assertEqual(140243640891303, ts.raw)
        self.assertEqual(42, ts.offset)

        # cannot go negative
        self.assertRaises(ValueError, timestamp.Timestamp, 1402436408.91203,
                          delta=-140243640891203)

    def test_int(self):
        expected = 1402437965
        test_values = (
            '1402437965.91203',
            '1402437965.91203_00000000',
            '1402437965.912030000',
            '1402437965.912030000_0000000000000',
            '000001402437965.912030000',
            '000001402437965.912030000_0000000000',
            '000001402437965.912030000_2000000001000001',
            1402437965.91203,
            1402437965.9120300000000000,
            1402437965.912029,
            1402437965.912029999999999999,
            timestamp.Timestamp(1402437965.91203),
            timestamp.Timestamp(1402437965.91203, offset=0),
            timestamp.Timestamp(1402437965.91203, offset=500),
            timestamp.Timestamp(1402437965.912029),
            timestamp.Timestamp(1402437965.91202999999999999, offset=0),
            timestamp.Timestamp(1402437965.91202999999999999, offset=300),
            timestamp.Timestamp('1402437965.91203'),
            timestamp.Timestamp('1402437965.91203', offset=0),
            timestamp.Timestamp('1402437965.91203', offset=400),
            timestamp.Timestamp('1402437965.912029'),
            timestamp.Timestamp('1402437965.912029', offset=0),
            timestamp.Timestamp('1402437965.912029', offset=200),
            timestamp.Timestamp('1402437965.912029999999999'),
            timestamp.Timestamp('1402437965.912029999999999', offset=0),
            timestamp.Timestamp('1402437965.912029999999999', offset=100),
        )
        for value in test_values:
            ts = timestamp.Timestamp(value)
            self.assertEqual(int(ts), expected)
            self.assertTrue(ts > expected)

    def test_float(self):
        expected = 1402438115.91203
        test_values = (
            '1402438115.91203',
            '1402438115.91203_00000000',
            '1402438115.912030000',
            '1402438115.912030000_0000000000000',
            '000001402438115.912030000',
            '000001402438115.912030000_0000000000',
            1402438115.91203,
            1402438115.9120300000000000,
            1402438115.912029,
            1402438115.912029999999999999,
            timestamp.Timestamp(1402438115.91203),
            timestamp.Timestamp(1402438115.91203, offset=0),
            timestamp.Timestamp(1402438115.91203, offset=500),
            timestamp.Timestamp(1402438115.912029),
            timestamp.Timestamp(1402438115.91202999999999999, offset=0),
            timestamp.Timestamp(1402438115.91202999999999999, offset=300),
            timestamp.Timestamp('1402438115.91203'),
            timestamp.Timestamp('1402438115.91203', offset=0),
            timestamp.Timestamp('1402438115.91203', offset=400),
            timestamp.Timestamp('1402438115.912029'),
            timestamp.Timestamp('1402438115.912029', offset=0),
            timestamp.Timestamp('1402438115.912029', offset=200),
            timestamp.Timestamp('1402438115.912029999999999'),
            timestamp.Timestamp('1402438115.912029999999999', offset=0),
            timestamp.Timestamp('1402438115.912029999999999', offset=100),
        )
        tolerance = 0.00001
        minimum = expected - tolerance
        maximum = expected + tolerance
        for value in test_values:
            ts = timestamp.Timestamp(value)
            self.assertTrue(float(ts) > minimum,
                            '%f is not bigger than %f given %r' % (
                                ts, minimum, value))
            self.assertTrue(float(ts) < maximum,
                            '%f is not smaller than %f given %r' % (
                                ts, maximum, value))
            # direct comparison of timestamp works too
            self.assertTrue(ts > minimum,
                            '%s is not bigger than %f given %r' % (
                                ts.normal, minimum, value))
            self.assertTrue(ts < maximum,
                            '%s is not smaller than %f given %r' % (
                                ts.normal, maximum, value))
            # ... even against strings
            self.assertTrue(ts > '%f' % minimum,
                            '%s is not bigger than %s given %r' % (
                                ts.normal, minimum, value))
            self.assertTrue(ts < '%f' % maximum,
                            '%s is not smaller than %s given %r' % (
                                ts.normal, maximum, value))

    def test_false(self):
        self.assertFalse(timestamp.Timestamp('0'))
        self.assertFalse(timestamp.Timestamp('0', offset=0))
        self.assertFalse(timestamp.Timestamp('0.0'))
        self.assertFalse(timestamp.Timestamp('0.0', offset=0))
        self.assertFalse(timestamp.Timestamp('00000000.00000000'))
        self.assertFalse(timestamp.Timestamp('00000000.00000000', offset=0))
        self.assertFalse(timestamp.Timestamp.zero())
        self.assertFalse(timestamp.Timestamp(0))

    def test_true(self):
        self.assertTrue(timestamp.Timestamp(0, version=2))
        self.assertTrue(timestamp.Timestamp(1))
        self.assertTrue(timestamp.Timestamp(1, offset=1))
        self.assertTrue(timestamp.Timestamp(0, offset=1))
        self.assertTrue(timestamp.Timestamp('1'))
        self.assertTrue(timestamp.Timestamp('1', offset=1))
        self.assertTrue(timestamp.Timestamp('0', offset=1))
        self.assertTrue(timestamp.Timestamp(1.1))
        self.assertTrue(timestamp.Timestamp(1.1, offset=1))
        self.assertTrue(timestamp.Timestamp(0.0, offset=1))
        self.assertTrue(timestamp.Timestamp('1.1'))
        self.assertTrue(timestamp.Timestamp('1.1', offset=1))
        self.assertTrue(timestamp.Timestamp('0.0', offset=1))
        self.assertTrue(timestamp.Timestamp(11111111.11111111))
        self.assertTrue(timestamp.Timestamp(11111111.11111111, offset=1))
        self.assertTrue(timestamp.Timestamp(00000000.00000000, offset=1))
        self.assertTrue(timestamp.Timestamp('11111111.11111111'))
        self.assertTrue(timestamp.Timestamp('11111111.11111111', offset=1))
        self.assertTrue(timestamp.Timestamp('00000000.00000000', offset=1))

    def _test_greater_no_offset(self, ts):
        older = float(ts) - 1
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443112.213252, '1402443112.213252', '1402443112.213252_ffff',
            older, '%f' % older, '%f_0000ffff' % older
        )
        for value in test_values:
            with self.subTest(value=value):
                other = timestamp.Timestamp(value)
                self.assertNotEqual(ts, other)  # sanity
                self.assertTrue(ts > value,
                                '%r is not greater than %r given %r' % (
                                    ts, value, value))
                self.assertTrue(ts > other,
                                '%r is not greater than %r given %r' % (
                                    ts, other, value))
                self.assertTrue(ts > other.normal,
                                '%r is not greater than %r given %r' % (
                                    ts, other.normal, value))
                self.assertTrue(ts > other.internal,
                                '%r is not greater than %r given %r' % (
                                    ts, other.internal, value))
                self.assertTrue(ts > float(other),
                                '%r is not greater than %r given %r' % (
                                    ts, float(other), value))
                self.assertTrue(ts > int(other),
                                '%r is not greater than %r given %r' % (
                                    ts, int(other), value))

    def test_greater_no_offset(self):
        now = time.time()
        ts = timestamp.Timestamp(now)
        self._test_greater_no_offset(ts)

    def test_greater_no_offset_legacy(self):
        now = time.time()
        ts = timestamp.Timestamp(str(now))
        self._test_greater_no_offset(ts)

    def _do_test_greater_with_offset(self, ts, test_values):
        for offset in range(1, 1000, 100):
            ts.increment_offset(offset)
            for value in test_values:
                with self.subTest(value=value, offset=offset):
                    with mock_timestamp_randint(0):
                        # force other to have zero jitter
                        other = timestamp.Timestamp(value)
                    self.assertNotEqual(ts, other)  # sanity
                    self.assertTrue(ts > value,
                                    '%r is not greater than %r given %r' % (
                                        ts, value, value))
                    self.assertTrue(ts > other,
                                    '%r is not greater than %r given %r' % (
                                        ts, other, value))
                    self.assertTrue(ts > other.normal,
                                    '%r is not greater than %r given %r' % (
                                        ts, other.normal, value))
                    self.assertTrue(ts > other.internal,
                                    '%r is not greater than %r given %r' % (
                                        ts, other.internal, value))
                    self.assertTrue(ts > float(other),
                                    '%r is not greater than %r given %r' % (
                                        ts, float(other), value))
                    self.assertTrue(ts > int(other),
                                    '%r is not greater than %r given %r' % (
                                        ts, int(other), value))

    def _test_greater_with_offset(self, version):
        # Part 1: use the natural time of the Python. This is deliciously
        # unpredictable, but completely legitimate and realistic. Finds bugs!
        now = time.time()
        older = now - 1
        if version == 1:
            now = str(now)
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443346.935174, '1402443346.93517', '1402443346.935169_ffff',
            older, now,
        )
        ts = timestamp.Timestamp(now)
        self._do_test_greater_with_offset(ts, test_values)
        # Part 2: Same as above, but with fixed time values that reproduce
        # specific corner cases.
        now = 1519830570.6949348
        older = now - 1
        if version == 1:
            now = str(now)
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443346.935174, '1402443346.93517', '1402443346.935169_ffff',
            older, now,
        )
        ts = timestamp.Timestamp(now)
        self._do_test_greater_with_offset(ts, test_values)
        # Part 3: The '%f' problem. Timestamps cannot be converted to %f
        # strings, then back to timestamps, then compared with originals.
        # You can only "import" a floating point representation once.
        now = 1519830570.6949348
        now = float('%f' % now)
        older = now - 1
        if version == 1:
            now = str(now)
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            older, '%f' % older, '%f_0000ffff' % older,
            now, '%f' % float(now), '%s_00000000' % now,
        )
        ts = timestamp.Timestamp(now)
        self._do_test_greater_with_offset(ts, test_values)

    def test_greater_with_offset(self):
        self._test_greater_with_offset(2)

    def test_greater_with_offset_legacy(self):
        self._test_greater_with_offset(1)

    def test_smaller_no_offset(self):
        now = time.time()
        newer = now + 1
        ts = timestamp.Timestamp(now)
        test_values = (
            9999999999.99999, '9999999999.99999', '9999999999.99999_ffff',
            newer, '%f' % newer, '%f_0000ffff' % newer,
        )
        for value in test_values:
            other = timestamp.Timestamp(value)
            self.assertNotEqual(ts, other)  # sanity
            self.assertTrue(ts < value,
                            '%r is not smaller than %r given %r' % (
                                ts, value, value))
            self.assertTrue(ts < other,
                            '%r is not smaller than %r given %r' % (
                                ts, other, value))
            self.assertTrue(ts < other.normal,
                            '%r is not smaller than %r given %r' % (
                                ts, other.normal, value))
            self.assertTrue(ts < other.internal,
                            '%r is not smaller than %r given %r' % (
                                ts, other.internal, value))
            self.assertTrue(ts < float(other),
                            '%r is not smaller than %r given %r' % (
                                ts, float(other), value))
            self.assertTrue(ts < int(other),
                            '%r is not smaller than %r given %r' % (
                                ts, int(other), value))

    def test_smaller_with_offset(self):
        now = time.time()
        newer = now + 1
        test_values = (
            9999999999.99999, '9999999999.99999', '9999999999.99999_ffff',
            newer, '%f' % newer, '%f_0000ffff' % newer,
        )
        for offset in range(1, 1000, 100):
            ts = timestamp.Timestamp(now, offset=offset)
            for value in test_values:
                other = timestamp.Timestamp(value)
                self.assertNotEqual(ts, other)  # sanity
                self.assertTrue(ts < value,
                                '%r is not smaller than %r given %r' % (
                                    ts, value, value))
                self.assertTrue(ts < other,
                                '%r is not smaller than %r given %r' % (
                                    ts, other, value))
                self.assertTrue(ts < other.normal,
                                '%r is not smaller than %r given %r' % (
                                    ts, other.normal, value))
                self.assertTrue(ts < other.internal,
                                '%r is not smaller than %r given %r' % (
                                    ts, other.internal, value))
                self.assertTrue(ts < float(other),
                                '%r is not smaller than %r given %r' % (
                                    ts, float(other), value))
                self.assertTrue(ts < int(other),
                                '%r is not smaller than %r given %r' % (
                                    ts, int(other), value))

    def test_cmp_with_none(self):
        self.assertGreater(timestamp.Timestamp(0), None)
        self.assertGreater(timestamp.Timestamp(1.0), None)
        self.assertGreater(timestamp.Timestamp(1.0, 42), None)

    def test_comparison_unsupported(self):
        ts = timestamp.Timestamp.now()
        with self.assertRaises(TypeError) as cm:
            self.assertGreater(ts, True)
        self.assertEqual("'>' not supported between instances of "
                         "'Timestamp' and 'bool'", str(cm.exception))

        with self.assertRaises(TypeError) as cm:
            self.assertGreater(ts, 'not a timestamp')
        self.assertEqual("'>' not supported between instances of "
                         "'Timestamp' and 'str'", str(cm.exception))

    def test_ordering(self):
        given = [
            '1402444820.62590_000000000000000a',
            '1402444820.62589_0000000000000001',
            '1402444821.52589_0000000000000004',
            '1402444920.62589_0000000000000004',
            '1402444821.62589_000000000000000a',
            '1402444821.72589_000000000000000a',
            '1402444920.62589_0000000000000002',
            '1402444820.62589_0000000000000002',
            '1402444820.62589_000000000000000a',
            '1402444820.62590_0000000000000004',
            '1402444920.62589_000000000000000a',
            '1402444820.62590_0000000000000002',
            '1402444821.52589_0000000000000002',
            '1402444821.52589_0000000000000000',
            '1402444920.62589',
            '1402444821.62589_0000000000000004',
            '1402444821.72589_0000000000000001',
            '1402444820.62590',
            '1402444820.62590_0000000000000001',
            '1402444820.62589_0000000000000004',
            '1402444821.72589_0000000000000000',
            '1402444821.52589_000000000000000a',
            '1402444821.72589_0000000000000004',
            '1402444821.62589',
            '1402444821.52589_0000000000000001',
            '1402444821.62589_0000000000000001',
            '1402444821.62589_0000000000000002',
            '1402444821.72589_0000000000000002',
            '1402444820.62589',
            '1402444920.62589_0000000000000001']
        expected = [
            '1402444820.62589',
            '1402444820.62589_0000000000000001',
            '1402444820.62589_0000000000000002',
            '1402444820.62589_0000000000000004',
            '1402444820.62589_000000000000000a',
            '1402444820.62590',
            '1402444820.62590_0000000000000001',
            '1402444820.62590_0000000000000002',
            '1402444820.62590_0000000000000004',
            '1402444820.62590_000000000000000a',
            '1402444821.52589',
            '1402444821.52589_0000000000000001',
            '1402444821.52589_0000000000000002',
            '1402444821.52589_0000000000000004',
            '1402444821.52589_000000000000000a',
            '1402444821.62589',
            '1402444821.62589_0000000000000001',
            '1402444821.62589_0000000000000002',
            '1402444821.62589_0000000000000004',
            '1402444821.62589_000000000000000a',
            '1402444821.72589',
            '1402444821.72589_0000000000000001',
            '1402444821.72589_0000000000000002',
            '1402444821.72589_0000000000000004',
            '1402444821.72589_000000000000000a',
            '1402444920.62589',
            '1402444920.62589_0000000000000001',
            '1402444920.62589_0000000000000002',
            '1402444920.62589_0000000000000004',
            '1402444920.62589_000000000000000a',
        ]
        # less visual version
        """
        now = time.time()
        given = [
            timestamp.Timestamp(now + i, offset=offset).internal
            for i in (0, 0.00001, 0.9, 1.0, 1.1, 100.0)
            for offset in (0, 1, 2, 4, 10)
        ]
        expected = [t for t in given]
        random.shuffle(given)
        """
        self.assertEqual(len(given), len(expected))  # sanity
        timestamps = [timestamp.Timestamp(t) for t in given]
        # our expected values don't include insignificant offsets
        with mock.patch('swift.common.utils.timestamp.FORCE_INTERNAL',
                        new=False):
            self.assertEqual(
                [t.internal for t in sorted(timestamps)], expected)
            # string sorting works as well
            self.assertEqual(
                sorted([t.internal for t in timestamps]), expected)

    def test_hashable(self):
        ts_0 = timestamp.Timestamp('1402444821.72589')
        ts_0_also = timestamp.Timestamp('1402444821.72589')
        self.assertEqual(ts_0, ts_0_also)  # sanity
        self.assertEqual(hash(ts_0), hash(ts_0_also))
        d = {ts_0: 'whatever'}
        self.assertIn(ts_0, d)  # sanity
        self.assertIn(ts_0_also, d)

    def test_out_of_range_comparisons(self):
        now = timestamp.Timestamp.now()

        def check_is_later(val):
            self.assertTrue(now != val)
            self.assertFalse(now == val)
            self.assertTrue(now <= val)
            self.assertTrue(now < val)
            self.assertTrue(val > now)
            self.assertTrue(val >= now)

        check_is_later(1e30)
        check_is_later(1579753284000)  # someone gave us ms instead of s!
        check_is_later('1579753284000')
        check_is_later(b'1e15')
        check_is_later(u'1.e+10_f')

        def check_is_earlier(val):
            self.assertTrue(now != val)
            self.assertFalse(now == val)
            self.assertTrue(now >= val)
            self.assertTrue(now > val)
            self.assertTrue(val < now)
            self.assertTrue(val <= now)

        check_is_earlier(-1)
        check_is_earlier(-0.1)
        check_is_earlier('-9999999')
        check_is_earlier(b'-9999.999')
        check_is_earlier(u'-1234_5678')

    @mock_timestamp_randint(0xa)
    def test_inversion_v2(self):
        ts = timestamp.Timestamp('0_2000000000000000')
        self.assertIsInstance(~ts, timestamp.Timestamp)
        self.assertEqual((~ts).internal, '9999999999.99999_dfffffffffffffff')

        ts = timestamp.Timestamp('123456.789_200000000a000000')
        self.assertIsInstance(~ts, timestamp.Timestamp)
        self.assertEqual(ts.internal, '0000123456.78900_200000000a000000')
        self.assertEqual((~ts).internal, '9999876543.21099_dffffffff5ffffff')

        ts = timestamp.Timestamp('123456.789_2000000000000000', offset=1)
        self.assertIsInstance(~ts, timestamp.Timestamp)
        self.assertEqual(ts.internal, '0000123456.78900_2000000000000001')
        self.assertEqual((~ts).internal, '9999876543.21099_dffffffffffffffe')

        ts = timestamp.Timestamp(123456.789, offset=1, version=2)
        self.assertIsInstance(~ts, timestamp.Timestamp)
        self.assertEqual(ts.internal, '0000123456.78900_200000000a000001')
        self.assertEqual(
            (~ts).internal, '9999876543.21099_dffffffff5fffffe')

    def test_inversion(self):
        ts = timestamp.Timestamp('0')
        inv_ts = ~ts
        self.assertIsInstance(inv_ts, timestamp.Timestamp)
        self.assertEqual((inv_ts).internal, '9999999999.99999')

        ts = timestamp.Timestamp('123456.789')
        inv_ts = ~ts
        self.assertIsInstance(inv_ts, timestamp.Timestamp)
        self.assertEqual(ts.internal, '0000123456.78900')
        self.assertEqual((inv_ts).internal, '9999876543.21099')

    def test_inversion_sorting(self):
        timestamps = sorted(timestamp.Timestamp(random.random() * 1e10)
                            for _ in range(20))
        self.assertEqual([x.internal for x in timestamps],
                         sorted(x.internal for x in timestamps))
        self.assertEqual([(~x).internal for x in reversed(timestamps)],
                         sorted((~x).internal for x in timestamps))

        ts = timestamp.Timestamp.now()
        self.assertGreater(~ts, ts)  # NB: will break around 2128

    def test_inversion_reversibility(self):
        def do_test(ts):
            inv = ~ts
            inv_inv = ~inv
            self.assertEqual(ts, inv_inv)
            self.assertEqual(ts.internal, inv_inv.internal)

            inv_inv_inv = ~inv_inv
            self.assertEqual(inv, inv_inv_inv)
            self.assertEqual(inv.normal, inv_inv_inv.normal)

        do_test(timestamp.Timestamp('1755077566.123456'))
        do_test(timestamp.Timestamp(1755077566.123456))
        do_test(timestamp.Timestamp(1755077566.123456, offset=1))
        do_test(timestamp.Timestamp.now())

    @mock_timestamp_randint(0xa)
    def test_normalized(self):
        ts = timestamp.Timestamp(1755077566.123456, offset=0xabcdef, version=2)
        self.assertEqual('1755077566.12346_200000000aabcdef', ts.internal)
        norm_ts = ts.normalized()
        self.assertIsInstance(norm_ts, timestamp.NormalTimestamp)
        self.assertEqual('1755077566.12346', norm_ts.normal)
        self.assertEqual(timestamp.NormalTimestamp(1755077566.123456), norm_ts)


class TestTimestampVsNormalTimestamp(unittest.TestCase):
    # verify relationship of different timestamp classes
    def setUp(self):
        self.norm_ts_older = timestamp.NormalTimestamp(1755077566.12345)
        self.norm_ts = timestamp.NormalTimestamp(1755077566.12346)
        self.ts_older = timestamp.Timestamp(1755077566.12345, version=2)
        self.ts_older_offset = timestamp.Timestamp(self.ts_older, offset=1)
        self.ts = timestamp.Timestamp(1755077566.12346, version=2)
        self.ts_no_jitter = timestamp.Timestamp('1755077566.12346')
        self.ts_offset = timestamp.Timestamp(self.ts, offset=1)

    def test_type(self):
        self.assertFalse(isinstance(self.norm_ts, type(self.ts)))
        self.assertFalse(isinstance(self.ts, type(self.norm_ts)))

    def test_zero(self):
        ts_zero = timestamp.Timestamp.zero()
        norm_ts_zero = timestamp.NormalTimestamp.zero()
        ts_0 = timestamp.Timestamp('0')
        self.assertEqual(norm_ts_zero, ts_zero)
        self.assertEqual(norm_ts_zero, ts_0)
        self.assertEqual(norm_ts_zero.normal, ts_zero.internal)
        self.assertEqual(norm_ts_zero.normal, ts_0.internal)
        self.assertEqual(norm_ts_zero.normal, ts_zero.normal)
        self.assertEqual(norm_ts_zero.normal, ts_0.normal)

    def test_equality(self):
        self.assertEqual(self.norm_ts, self.ts_no_jitter)
        self.assertEqual(self.norm_ts.internal, self.ts_no_jitter)
        self.assertEqual(self.norm_ts, self.ts_no_jitter.internal)

    def test_inequality(self):
        def do_test(a, b):
            self.assertNotEqual(a, b)
            self.assertNotEqual(a.internal, b)
            self.assertNotEqual(a, b.internal)

        do_test(self.norm_ts, self.ts)
        do_test(self.norm_ts, self.ts_offset)
        do_test(self.ts, self.ts_offset)
        do_test(self.ts, self.ts_older)
        do_test(self.norm_ts, self.ts_older)

    def test_lt(self):
        def do_test(a, b):
            self.assertLess(a, b)
            self.assertLess(a.internal, b)
            self.assertLess(a, b.internal)

        do_test(self.ts_older, self.ts)
        do_test(self.ts_older, self.norm_ts)
        do_test(self.norm_ts_older, self.ts)
        do_test(self.norm_ts_older, self.norm_ts)
        do_test(self.ts, self.ts_offset)
        do_test(self.norm_ts, self.ts_offset)
        do_test(self.ts_no_jitter, self.ts_offset)
        do_test(self.ts_no_jitter, self.ts)

    def test_gt(self):
        def do_test(a, b):
            self.assertGreater(a, b)
            self.assertGreater(a.internal, b)
            self.assertGreater(a, b.internal)

        do_test(self.ts, self.ts_older)
        do_test(self.norm_ts, self.ts_older)
        do_test(self.ts, self.norm_ts_older)
        do_test(self.norm_ts, self.norm_ts_older)
        do_test(self.ts_offset, self.ts)
        do_test(self.ts_offset, self.norm_ts)
        do_test(self.ts, self.ts_no_jitter)


class TestTimestampEncoding(unittest.TestCase):

    @mock_timestamp_randint(0xa)
    def setUp(self):
        # the _norm is a zero-jitter version of each timestamp used when we
        # need to assert that jitter has been lost from ctype and meta
        # timestamps in the encoding/decoding round-trip
        t0 = timestamp.Timestamp(0.0, version=2)
        t0_norm = timestamp.Timestamp('0.0')
        t1 = timestamp.Timestamp(997.9996, version=2)
        t1_norm = timestamp.Timestamp('997.9996')
        t2 = timestamp.Timestamp(999, version=2)
        t2_norm = timestamp.Timestamp('999')
        t3 = timestamp.Timestamp(1000, 24, version=2)
        t4 = timestamp.Timestamp(1001, version=2)
        t4_norm = timestamp.Timestamp('1001')
        t5 = timestamp.Timestamp(1002.00040, version=2)
        t5_norm = timestamp.Timestamp('1002.00040')

        # encodings that are expected when explicit = False
        self.non_explicit_encodings = (
            ('0000001000.00000_200000000a000018', (t3, t3, t3)),
            ('0000001000.00000_200000000a000018', (t3, t3, None)),
        )

        # mappings that are expected when explicit = True
        self.explicit_encodings = (
            ('0000001000.00000_200000000a000018+0+0', (t3, t3, t3)),
            ('0000001000.00000_200000000a000018+0', (t3, t3, None)),
        )

        # mappings that are expected when explicit = True or False
        self.encodings = (
            ('0000001000.00000_200000000a000018+0+186a0', (t3, t3, t4)),
            ('0000001000.00000_200000000a000018+186a0+186c8', (t3, t4, t5)),
            ('0000001000.00000_200000000a000018-186a0+0', (t3, t2, t2)),
            ('0000001000.00000_200000000a000018+0-186a0', (t3, t3, t2)),
            ('0000001000.00000_200000000a000018-186a0-186c8', (t3, t2, t1)),
            ('0000001000.00000_200000000a000018', (t3, None, None)),
            ('0000001000.00000_200000000a000018+186a0', (t3, t4, None)),
            ('0000001000.00000_200000000a000018-186a0', (t3, t2, None)),
            ('0000001000.00000_200000000a000018', (t3, None, t1)),
            ('0000001000.00000_200000000a000018-5f5e100', (t3, t0, None)),
            ('0000001000.00000_200000000a000018+0-5f5e100', (t3, t3, t0)),
            ('0000001000.00000_200000000a000018-5f5e100+5f45a60', (t3, t0, t2))
        )

        # decodings that are expected when explicit = False
        self.non_explicit_decodings = (
            ('0000001000.00000_200000000a000018',
             (t3, t3, t3)),
            ('0000001000.00000_200000000a000018+186a0',
             (t3, t4_norm, t4_norm)),
            ('0000001000.00000_200000000a000018-186a0',
             (t3, t2_norm, t2_norm)),
            ('0000001000.00000_200000000a000018+186a0',
             (t3, t4_norm, t4_norm)),
            ('0000001000.00000_200000000a000018-186a0',
             (t3, t2_norm, t2_norm)),
            ('0000001000.00000_200000000a000018-5f5e100',
             (t3, t0_norm, t0_norm))
        )

        # decodings that are expected when explicit = True
        self.explicit_decodings = (
            ('0000001000.00000_200000000a000018+0+0', (t3, t3, t3)),
            ('0000001000.00000_200000000a000018+0', (t3, t3, None)),
            ('0000001000.00000_200000000a000018', (t3, None, None)),
            ('0000001000.00000_200000000a000018+186a0', (t3, t4_norm, None)),
            ('0000001000.00000_200000000a000018-186a0', (t3, t2_norm, None)),
            ('0000001000.00000_200000000a000018-5f5e100', (t3, t0_norm, None)),
        )

        # decodings that are expected when explicit = True or False
        self.decodings = (
            ('0000001000.00000_200000000a000018+0+186a0',
             (t3, t3, t4_norm)),
            ('0000001000.00000_200000000a000018+186a0+186c8',
             (t3, t4_norm, t5_norm)),
            ('0000001000.00000_200000000a000018-186a0+0',
             (t3, t2_norm, t2_norm)),
            ('0000001000.00000_200000000a000018+0-186a0',
             (t3, t3, t2_norm)),
            ('0000001000.00000_200000000a000018-186a0-186c8',
             (t3, t2_norm, t1_norm)),
            ('0000001000.00000_200000000a000018-5f5e100+5f45a60',
             (t3, t0_norm, t2_norm))
        )

    def _assertEqual(self, expected, actual, test):
        self.assertEqual(expected, actual,
                         'Got %s but expected %s for parameters %s'
                         % (actual, expected, test))

    def test_encoding(self):
        for test in self.explicit_encodings:
            actual = timestamp.encode_timestamps(test[1][0], test[1][1],
                                                 test[1][2], True)
            self._assertEqual(test[0], actual, test[1])
        for test in self.non_explicit_encodings:
            actual = timestamp.encode_timestamps(test[1][0], test[1][1],
                                                 test[1][2], False)
            self._assertEqual(test[0], actual, test[1])
        for explicit in (True, False):
            for test in self.encodings:
                actual = timestamp.encode_timestamps(test[1][0], test[1][1],
                                                     test[1][2], explicit)
                self._assertEqual(test[0], actual, test[1])

    def test_decoding(self):
        for test in self.explicit_decodings:
            actual = timestamp.decode_timestamps(test[0], True)
            self._assertEqual(test[1], actual, test[0])
        for test in self.non_explicit_decodings:
            actual = timestamp.decode_timestamps(test[0], False)
            self._assertEqual(test[1], actual, test[0])
        for explicit in (True, False):
            for test in self.decodings:
                actual = timestamp.decode_timestamps(test[0], explicit)
                self._assertEqual(test[1], actual, test[0])


class TestModuleFunctions(BaseUnitTestCase):
    @mock_timestamp_randint(0xa)
    def test_normalize_timestamp(self):
        # Test swift.common.utils.timestamp.normalize_timestamp
        self.assertEqual(timestamp.normalize_timestamp('1253327593.48174'),
                         "1253327593.48174")
        self.assertEqual(timestamp.normalize_timestamp(1253327593.48174),
                         "1253327593.48174")
        self.assertEqual(timestamp.normalize_timestamp('1253327593.48'),
                         "1253327593.48000")
        self.assertEqual(timestamp.normalize_timestamp(1253327593.48),
                         "1253327593.48000")
        self.assertEqual(timestamp.normalize_timestamp('253327593.48'),
                         "0253327593.48000")
        self.assertEqual(timestamp.normalize_timestamp(253327593.48),
                         "0253327593.48000")
        self.assertEqual(timestamp.normalize_timestamp('1253327593'),
                         "1253327593.00000")
        self.assertEqual(timestamp.normalize_timestamp(1253327593),
                         "1253327593.00000")
        self.assertEqual(timestamp.normalize_timestamp(
            timestamp.Timestamp(1253327593, offset=0xabcdef).internal),
            "1253327593.00000")
        self.assertEqual(timestamp.normalize_timestamp(0), "0000000000.00000")
        self.assertRaises(ValueError, timestamp.normalize_timestamp, '')
        self.assertRaises(ValueError, timestamp.normalize_timestamp, 'abc')

    def test_normalize_delete_at_timestamp(self):
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(1253327593),
            '1253327593')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(1253327593.67890),
            '1253327593')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('1253327593'),
            '1253327593')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('1253327593.67890'),
            '1253327593')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(-1253327593),
            '0000000000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(-1253327593.67890),
            '0000000000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('-1253327593'),
            '0000000000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('-1253327593.67890'),
            '0000000000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(71253327593),
            '9999999999')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(71253327593.67890),
            '9999999999')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('71253327593'),
            '9999999999')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('71253327593.67890'),
            '9999999999')
        with self.assertRaises(TypeError):
            timestamp.normalize_delete_at_timestamp(None)
        with self.assertRaises(ValueError):
            timestamp.normalize_delete_at_timestamp('')
        with self.assertRaises(ValueError):
            timestamp.normalize_delete_at_timestamp('abc')

    def test_normalize_delete_at_timestamp_high_precision(self):
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(1253327593, True),
            '1253327593.00000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(1253327593.67890, True),
            '1253327593.67890')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('1253327593', True),
            '1253327593.00000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('1253327593.67890', True),
            '1253327593.67890')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(-1253327593, True),
            '0000000000.00000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(-1253327593.67890, True),
            '0000000000.00000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('-1253327593', True),
            '0000000000.00000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('-1253327593.67890', True),
            '0000000000.00000')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(71253327593, True),
            '9999999999.99999')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp(71253327593.67890, True),
            '9999999999.99999')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('71253327593', True),
            '9999999999.99999')
        self.assertEqual(
            timestamp.normalize_delete_at_timestamp('71253327593.67890', True),
            '9999999999.99999')
        with self.assertRaises(TypeError):
            timestamp.normalize_delete_at_timestamp(None, True)
        with self.assertRaises(ValueError):
            timestamp.normalize_delete_at_timestamp('', True)
        with self.assertRaises(ValueError):
            timestamp.normalize_delete_at_timestamp('abc', True)

    def test_last_modified_date_to_timestamp(self):
        self.assertEqual(
            timestamp.last_modified_date_to_timestamp(
                '1970-01-01T00:00:00.000000'),
            timestamp.Timestamp.zero()),
        self.assertEqual(
            timestamp.last_modified_date_to_timestamp(
                '2014-02-28T23:22:36.698390'),
            timestamp.Timestamp('1393629756.698390')),
        self.assertEqual(
            timestamp.last_modified_date_to_timestamp(
                '2011-03-19T04:03:00.604554'),
            timestamp.NormalTimestamp('1300507380.604554')),

    def test_last_modified_date_to_timestamp_when_system_not_UTC(self):
        try:
            old_tz = os.environ.get('TZ')
            # Western Argentina Summer Time. Found in glibc manual; this
            # timezone always has a non-zero offset from UTC, so this test is
            # always meaningful.
            os.environ['TZ'] = 'WART4WARST,J1/0,J365/25'

            self.assertEqual(timestamp.last_modified_date_to_timestamp(
                '1970-01-01T00:00:00.000000'),
                timestamp.Timestamp.zero())

        finally:
            if old_tz is not None:
                os.environ['TZ'] = old_tz
            else:
                os.environ.pop('TZ')

    def test_timestamp2(self):
        with mock_timestamp_randint(0xa), \
                mock.patch('swift.common.utils.timestamp.time.time',
                           return_value=1234567890.12345):
            ts = timestamp.timestamp2()
        self.assertIsInstance(ts, timestamp.Timestamp)
        self.assertEqual(1234567890.12345, float(ts))
        self.assertEqual(0x200000000a000000, ts.hex_part)
        self.assertEqual('1234567890.12345_200000000a000000', ts.internal)

    def test_generate_timestamp(self):
        self.assert_valid_extended_timestamp(
            timestamp.generate_timestamp('object', 'PUT'))

        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('object', 'POST'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('object', 'DELETE'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('object', 'GET'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('object', 'HEAD'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('container', 'PUT'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('container', 'POST'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('container', 'DELETE'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('container', 'GET'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('container', 'HEAD'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('account', 'PUT'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('account', 'POST'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('account', 'DELETE'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('account', 'GET'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp('account', 'HEAD'))
        self.assert_valid_normal_timestamp(
            timestamp.generate_timestamp(None, 'GET'))

    def test_parse_timestamp(self):
        def do_test_extended_timestamp(resource_type, request_method):
            ts_str = self.ts().internal
            actual = timestamp.parse_timestamp(
                ts_str, resource_type, request_method)
            self.assertEqual(ts_str, actual)
            self.assertIsInstance(actual, timestamp.Timestamp)

        do_test_extended_timestamp('object', 'PUT')
        do_test_extended_timestamp('object', 'DELETE')
        do_test_extended_timestamp('object', 'GET')
        do_test_extended_timestamp('object', 'HEAD')
        do_test_extended_timestamp('container', 'GET')
        do_test_extended_timestamp('container', 'HEAD')
        do_test_extended_timestamp('account', 'GET')
        do_test_extended_timestamp('account', 'HEAD')

        def do_test_normal_timestamp(resource_type, request_method):
            norm_ts_str = self.normal_ts().normal
            actual = timestamp.parse_timestamp(
                norm_ts_str, resource_type, request_method)
            self.assertEqual(norm_ts_str, actual)
            self.assertIsInstance(actual, timestamp.NormalTimestamp)

        do_test_normal_timestamp('object', 'POST')
        do_test_normal_timestamp('container', 'PUT')
        do_test_normal_timestamp('container', 'POST')
        do_test_normal_timestamp('container', 'DELETE')
        do_test_normal_timestamp('account', 'PUT')
        do_test_normal_timestamp('account', 'POST')
        do_test_normal_timestamp('account', 'DELETE')
        do_test_normal_timestamp(None, 'DELETE')

    def test_parse_timestamp_errors(self):
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp('not a timestamp', 'object', 'PUT')

        ts_str = self.ts().internal
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'object', 'POST')
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'container', 'PUT')
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'container', 'POST')
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'container', 'DELETE')
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'account', 'PUT')
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'account', 'POST')
        with self.assertRaises(ValueError):
            timestamp.parse_timestamp(ts_str, 'account', 'DELETE')
