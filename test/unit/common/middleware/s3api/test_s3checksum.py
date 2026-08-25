# Copyright (c) 2026 NVIDIA
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

import base64
from io import BytesIO
import unittest

from swift.common.middleware.s3api.s3checksum import \
    CHECKSUMS_BY_HEADER, COMPOSITE_MPU_CHECKSUM_HEADERS, \
    FULL_OBJECT_MPU_CHECKSUM_HEADERS, \
    ChecksummingInput, checksum_algorithm_from_header, \
    checksum_header_from_algorithm, checksum_sysmeta_headers, \
    checksum_sysmeta_to_response_headers, checksum_sysmeta_values, \
    format_supported_checksum_algorithms, get_checksum_hasher, \
    normalize_checksum_algorithm, normalize_checksum_type, \
    normalize_checksum_mode, split_composite_checksum, \
    validate_checksum_value
from swift.common.middleware.s3api.exception import \
    S3InputChecksumMismatch, S3InputChecksumTrailerInvalid
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import checksum


class TestChecksummingInput(unittest.TestCase):
    def test_calculated_checksum_too_short(self):
        body = b'123456789'
        wrapped = ChecksummingInput(
            BytesIO(body), len(body) + 1, checksum.crc32(),
            'x-amz-checksum-crc32', None)

        self.assertIsNone(wrapped.checksum_value)
        self.assertEqual(body, wrapped.read(len(body)))
        self.assertIsNone(wrapped.checksum_value)
        with self.assertRaises(S3InputChecksumMismatch):
            wrapped.read()
        self.assertIsNone(wrapped.checksum_value)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_checksum_value(self):
        body = b'123456789'
        expected_checksum = base64.b64encode(
            checksum.crc32(body).digest()).decode('ascii')
        for content_length, checksum_source in (
                (len(body), {'x-amz-checksum-crc32': expected_checksum}),
                (len(body), None), (None, None)):
            with self.subTest(content_length=content_length,
                              checksum_source=checksum_source):
                wrapped = ChecksummingInput(
                    BytesIO(body), content_length, checksum.crc32(),
                    'x-amz-checksum-crc32', checksum_source)

                self.assertEqual('x-amz-checksum-crc32', wrapped.checksum_key)
                self.assertEqual('CRC32', wrapped.algorithm)
                self.assertIsNone(wrapped.checksum_value)
                # Retain the value only when the input is complete.
                self.assertEqual(b'1234', wrapped.read(4))
                self.assertIsNone(wrapped.checksum_value)
                self.assertEqual(b'56789', wrapped.read(5))
                if content_length is None:
                    # An exact-size read does not signal EOF.
                    self.assertIsNone(wrapped.checksum_value)
                    self.assertEqual(b'', wrapped.read(1))
                self.assertEqual(expected_checksum, wrapped.checksum_value)
                self.assertFalse(wrapped.wsgi_input.closed)

    def test_mismatched_checksum(self):
        body = b'123456789'
        mismatched_checksum = base64.b64encode(
            checksum.crc32(b'not the body').digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': mismatched_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumMismatch) as raised:
            wrapped.read()
        self.assertEqual(('CRC32',), raised.exception.args)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_invalid_checksum_source(self):
        body = b'123456789'
        checksum_source = {
            'x-amz-checksum-crc32': 'not-a-valid-checksum',
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumTrailerInvalid) as raised:
            wrapped.read()
        self.assertEqual('x-amz-checksum-crc32', raised.exception.trailer)

    def test_empty_body(self):
        expected_checksum = base64.b64encode(
            checksum.crc32(b'').digest()).decode('ascii')
        for content_length, checksum_source in (
                (0, {'x-amz-checksum-crc32': expected_checksum}),
                (0, None), (None, None)):
            with self.subTest(content_length=content_length,
                              checksum_source=checksum_source):
                wrapped = ChecksummingInput(
                    BytesIO(b''), content_length, checksum.crc32(),
                    'x-amz-checksum-crc32', checksum_source)

                self.assertIsNone(wrapped.checksum_value)
                self.assertEqual(b'', wrapped.read())
                self.assertEqual(expected_checksum, wrapped.checksum_value)
                self.assertFalse(wrapped.wsgi_input.closed)

    def test_missing_checksum_source(self):
        # the client promised a checksum trailer but never sent one
        body = b'123456789'
        wrapped = ChecksummingInput(
            BytesIO(body), len(body), checksum.crc32(),
            'x-amz-checksum-crc32', {})

        with self.assertRaises(S3InputChecksumTrailerInvalid) as raised:
            wrapped.read()
        self.assertEqual('x-amz-checksum-crc32', raised.exception.trailer)

    def test_body_is_too_short(self):
        body = b'123456789'
        expected_checksum = base64.b64encode(
            checksum.crc32(body).digest()).decode('ascii')
        checksum_source = {
            'x-amz-checksum-crc32': expected_checksum,
        }
        wrapped = ChecksummingInput(
            BytesIO(body), len(body) + 1, checksum.crc32(),
            'x-amz-checksum-crc32', checksum_source)

        with self.assertRaises(S3InputChecksumMismatch) as raised:
            wrapped.read()
        self.assertEqual(('CRC32',), raised.exception.args)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_body_is_too_long(self):
        body = b'123456789'
        expected_checksum = base64.b64encode(
            checksum.crc32(body).digest()).decode('ascii')
        for checksum_source in (
                {'x-amz-checksum-crc32': expected_checksum}, None):
            with self.subTest(checksum_source=checksum_source):
                wrapped = ChecksummingInput(
                    BytesIO(body), len(body) - 1, checksum.crc32(),
                    'x-amz-checksum-crc32', checksum_source)

                self.assertIsNone(wrapped.checksum_value)
                with self.assertRaises(S3InputChecksumMismatch) as raised:
                    wrapped.read()
                self.assertEqual(('CRC32',), raised.exception.args)
                self.assertIsNone(wrapped.checksum_value)
                self.assertTrue(wrapped.wsgi_input.closed)


class TestGetChecksumHasher(unittest.TestCase):
    def test_get_checksum_hasher(self):
        for algo in ('crc32', 'crc32c', 'crc64nvme', 'sha1', 'sha256'):
            with self.subTest(algo=algo):
                hasher = get_checksum_hasher('x-amz-checksum-%s' % algo)
                self.assertEqual(algo, hasher.name)

    def test_get_checksum_hasher_invalid(self):
        # the algorithm is not recognised
        def do_test(crc):
            with self.assertRaises(KeyError):
                get_checksum_hasher('x-amz-checksum-%s' % crc)

        do_test('nonsense')
        do_test('')


class TestValidateChecksumValue(unittest.TestCase):
    def test_valid_value(self):
        hasher = checksum.crc32(b'123456789')
        self.assertEqual(4, len(hasher.digest()))
        b64digest = base64.b64encode(hasher.digest()).decode('ascii')
        self.assertEqual(hasher.digest(),
                         validate_checksum_value(hasher, b64digest))

    def test_wrong_size_value(self):
        hasher = checksum.crc32(b'123456789')
        b64digest = base64.b64encode(b'x' * 8).decode('ascii')
        with self.assertRaises(ValueError):
            validate_checksum_value(hasher, b64digest)

    def test_not_base64_value(self):
        hasher = checksum.crc32(b'123456789')
        with self.assertRaises(ValueError):
            validate_checksum_value(hasher, 'not-a-valid-checksum')


class TestModuleFunctions(unittest.TestCase):
    def test_normalize_checksum_algorithm(self):
        self.assertEqual('CRC32', normalize_checksum_algorithm('crc32'))
        self.assertEqual('CRC32', normalize_checksum_algorithm('  CRC32  '))
        # note: the normalize function does not *validate* the value; S3
        # rejects hyphenated spellings with InvalidRequest
        self.assertEqual('CRC-32', normalize_checksum_algorithm('crc-32'))

    def test_normalize_checksum_mode(self):
        self.assertIsNone(normalize_checksum_mode(None))
        self.assertEqual('ENABLED', normalize_checksum_mode('enabled'))

    def test_normalize_checksum_type(self):
        self.assertEqual('COMPOSITE', normalize_checksum_type('composite'))
        self.assertEqual('FULL_OBJECT', normalize_checksum_type(
            'full_object'))

    def test_mpu_checksum_capabilities_come_from_checksum_headers(self):
        self.assertEqual(
            ('x-amz-checksum-crc32', 'x-amz-checksum-crc32c',
             'x-amz-checksum-crc64nvme'),
            FULL_OBJECT_MPU_CHECKSUM_HEADERS)
        self.assertEqual(
            ('x-amz-checksum-crc32', 'x-amz-checksum-crc32c',
             'x-amz-checksum-sha1', 'x-amz-checksum-sha256'),
            COMPOSITE_MPU_CHECKSUM_HEADERS)
        self.assertTrue(
            set(FULL_OBJECT_MPU_CHECKSUM_HEADERS).issubset(
                CHECKSUMS_BY_HEADER))
        self.assertTrue(
            set(COMPOSITE_MPU_CHECKSUM_HEADERS).issubset(
                CHECKSUMS_BY_HEADER))

    def test_format_supported_checksum_algorithms(self):
        self.assertEqual(
            '[CRC32, CRC32C, CRC64NVME, SHA1, SHA256]',
            format_supported_checksum_algorithms())

    def test_checksum_sysmeta_headers(self):
        sysmeta_headers = checksum_sysmeta_headers(
            'crc32', 'DUoRhQ==', 'FULL_OBJECT')
        self.assertEqual(HeaderKeyDict({
            'x-object-sysmeta-s3api-checksum-algorithm':
                'CRC32',
            'x-object-sysmeta-s3api-checksum-value': 'DUoRhQ==',
            'x-object-sysmeta-s3api-checksum-type': 'FULL_OBJECT',
        }), sysmeta_headers)
        self.assertEqual(('CRC32', 'DUoRhQ==', 'FULL_OBJECT'),
                         checksum_sysmeta_values(sysmeta_headers))

    def test_checksum_sysmeta_to_response_headers(self):
        for algorithm, value in (
            ('CRC32', 'DUoRhQ=='),
            ('SHA256',
                'ZTNiMGM0NDI5OGZjMWMxNDlhZmJmNGM4OTk2ZmI5MjQy'
                'N2FlNDFlNDY0OWI5MzRjYTQ5NTk5MWI3ODUyYjg1NQ=='),
        ):
            with self.subTest(algorithm=algorithm):
                sysmeta = {
                    'x-object-sysmeta-s3api-checksum-algorithm': algorithm,
                    'x-object-sysmeta-s3api-checksum-value': value,
                    'x-object-sysmeta-s3api-checksum-type': 'FULL_OBJECT',
                }
                self.assertEqual({
                    'x-amz-checksum-type': 'FULL_OBJECT',
                    'x-amz-checksum-' + algorithm.lower(): value,
                }, checksum_sysmeta_to_response_headers(sysmeta))

    def test_checksum_sysmeta_to_response_headers_missing_sysmeta(self):
        for missing in ('type', 'algorithm', 'value'):
            with self.subTest(missing=missing):
                sysmeta = {
                    'x-object-sysmeta-s3api-checksum-algorithm': 'CRC32',
                    'x-object-sysmeta-s3api-checksum-value': 'DUoRhQ==',
                    'x-object-sysmeta-s3api-checksum-type': 'FULL_OBJECT',
                }
                del sysmeta['x-object-sysmeta-s3api-checksum-' + missing]
                self.assertEqual(
                    {}, checksum_sysmeta_to_response_headers(sysmeta))

        # missing all checksum sysmeta
        sysmeta = {'x-object-sysmeta-s3api-etag': 'an-etag'}
        self.assertEqual({}, checksum_sysmeta_to_response_headers(sysmeta))

    def test_checksum_header_from_algorithm(self):
        self.assertEqual(
            'x-amz-checksum-crc32', checksum_header_from_algorithm('crc32'))
        self.assertEqual(
            'x-amz-checksum-crc-32', checksum_header_from_algorithm('crc-32'))
        for algorithm in ('CRC32', 'CRC32C', 'CRC64NVME', 'SHA1', 'SHA256'):
            with self.subTest(algorithm=algorithm):
                checksum_header = checksum_header_from_algorithm(algorithm)
                self.assertEqual(
                    'x-amz-checksum-%s' % algorithm.lower(), checksum_header)
                self.assertEqual(
                    algorithm,
                    checksum_algorithm_from_header(checksum_header))

    def test_checksum_algorithm_from_header(self):
        self.assertEqual(
            'CRC32', checksum_algorithm_from_header(
                'x-amz-checksum-crc32'))

    def test_split_composite_checksum(self):
        self.assertEqual(
            ('DUoRhQ==', None), split_composite_checksum('DUoRhQ=='))
        self.assertEqual(
            ('DUoRhQ==', 1), split_composite_checksum('DUoRhQ==-1'))
        self.assertEqual(
            ('DUoRhQ==', 10000), split_composite_checksum('DUoRhQ==-10000'))
        # beyond being non-empty, the digest is not inspected
        self.assertEqual(
            ('not!base64', None), split_composite_checksum('not!base64'))

    def test_split_composite_checksum_bad_part_count(self):
        # A part count of zero or less is impossible, and anything that is not
        # a plain integer never was a part count.
        for value in ('DUoRhQ==-0', 'DUoRhQ==--1', 'DUoRhQ==-abc',
                      'DUoRhQ==-', 'DUoRhQ==-1.0', 'DUoRhQ==-+1',
                      'DUoRhQ==-1-2'):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    split_composite_checksum(value)

    def test_split_composite_checksum_missing_digest(self):
        # A suffix with nothing in front of it is not a checksum at all.
        for value in ('', '-', '-1'):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    split_composite_checksum(value)

    def test_checksum_sysmeta_headers_without_type(self):
        self.assertEqual(HeaderKeyDict({
            'x-object-sysmeta-s3api-checksum-algorithm': 'CRC32',
            'x-object-sysmeta-s3api-checksum-value': 'DUoRhQ==',
        }), checksum_sysmeta_headers(
            'CRC32', 'DUoRhQ==', None))

    def test_checksum_sysmeta_headers_without_value(self):
        self.assertEqual(HeaderKeyDict({
            'x-object-sysmeta-s3api-checksum-algorithm': 'CRC32',
            'x-object-sysmeta-s3api-checksum-type': 'COMPOSITE',
        }), checksum_sysmeta_headers(
            'CRC32', None, checksum_type='COMPOSITE'))


if __name__ == '__main__':
    unittest.main()
