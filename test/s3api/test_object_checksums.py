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

import binascii
import botocore
import hashlib
from unittest import SkipTest

from swift.common.utils import base64_str
from swift.common.utils.checksum import crc32, crc32c
from test.s3api import (BaseS3TestCaseWithBucket,
                        skip_if_checksum_persistence_disabled)

TEST_BODY = b'123456789'


def boto_at_least(*version):
    return tuple(int(x) for x in botocore.__version__.split('.')) >= version


def inject_invalid_checksum_mode(params, **kwargs):
    """Inject an invalid checksum mode into a botocore request."""
    params['headers']['x-amz-checksum-mode'] = 'INVALID'


def inject_lowercase_checksum_mode(params, **kwargs):
    """Inject a lowercase checksum mode into a botocore request."""
    params['headers']['x-amz-checksum-mode'] = 'enabled'


def capture_prepared_headers(captured):
    """Return a botocore before-send callback that saves request headers."""
    def capture(request, **kwargs):
        captured.update({
            key.lower(): (value.decode('ascii')
                          if isinstance(value, bytes) else value)
            for key, value in request.headers.items()
        })
    return capture


class ObjectChecksumMixin(object):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

    def assert_error(self, resp, err_code, err_msg, obj_name, **extra):
        self.assertEqual(400, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(err_code, resp['Error']['Code'])
        self.assertEqual(err_msg, resp['Error']['Message'])
        self.assertEqual({k: resp['Error'].get(k) for k in extra}, extra)

        # Sanity check: object was not created
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.head_object(Bucket=self.bucket_name, Key=obj_name)
        resp = caught.exception.response
        self.assertEqual(404, resp['ResponseMetadata']['HTTPStatusCode'])

    def _assert_checksum_response(self, resp):
        """Check a successful response and its checksum headers."""
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']
        self.assertEqual(self.EXPECTED, headers.get(self.CHECKSUM_HDR))
        self.assertEqual('FULL_OBJECT', headers.get('x-amz-checksum-type'))
        self.assertEqual(self.EXPECTED, resp[self.CHECKSUM_FIELD])
        self.assertEqual('FULL_OBJECT', resp['ChecksumType'])

    def _assert_checksum_persistence(self, obj_name):
        """Check HEAD and GET for an object's stored checksum."""
        for method, read_object in (
                ('HEAD', self.client.head_object),
                ('GET', self.client.get_object)):
            with self.subTest(method=method):
                resp = read_object(Bucket=self.bucket_name, Key=obj_name,
                                   ChecksumMode='ENABLED')
                self._assert_checksum_response(resp)
                if method == 'GET':
                    self.assertEqual(TEST_BODY, resp['Body'].read())

    @skip_if_checksum_persistence_disabled
    def test_let_sdk_compute(self):
        obj_name = self.create_name(self.ALGORITHM + '-sdk')
        resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=TEST_BODY,
            ChecksumAlgorithm=self.ALGORITHM,
        )
        self._assert_checksum_response(resp)
        self._assert_checksum_persistence(obj_name)

    @skip_if_checksum_persistence_disabled
    def test_good_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-with-algo-header')
        resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=TEST_BODY,
            ChecksumAlgorithm=self.ALGORITHM,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED}
        )
        self._assert_checksum_response(resp)
        self._assert_checksum_persistence(obj_name)

    @skip_if_checksum_persistence_disabled
    def test_good_checksum_no_algorithm_header(self):
        obj_name = self.create_name(self.ALGORITHM + '-no-algo-header')
        resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED}
        )
        self._assert_checksum_response(resp)
        self._assert_checksum_persistence(obj_name)

        for operation, read_object in (
                ('GetObject', self.client.get_object),
                ('HeadObject', self.client.head_object)):
            with self.subTest(operation=operation):
                event_name = 'before-call.s3.' + operation
                self.client.meta.events.register(
                    event_name, inject_lowercase_checksum_mode)
                try:
                    lowercase_resp = read_object(
                        Bucket=self.bucket_name, Key=obj_name,
                        ChecksumMode='ENABLED')
                finally:
                    self.client.meta.events.unregister(
                        event_name, inject_lowercase_checksum_mode)
                self._assert_checksum_response(lowercase_resp)
                if operation == 'GetObject':
                    self.assertEqual(TEST_BODY, lowercase_resp['Body'].read())

                range_resp = read_object(
                    Bucket=self.bucket_name, Key=obj_name,
                    Range='bytes=0-3', ChecksumMode='ENABLED')
                self.assertEqual(
                    206, range_resp['ResponseMetadata']['HTTPStatusCode'])
                if operation == 'GetObject':
                    self.assertEqual(TEST_BODY[:4], range_resp['Body'].read())
                self.assertEqual('bytes 0-3/9', range_resp['ContentRange'])
                self.assertEqual(4, range_resp['ContentLength'])
                range_headers = range_resp['ResponseMetadata']['HTTPHeaders']
                self.assertNotIn(self.CHECKSUM_HDR, range_headers)
                self.assertNotIn('x-amz-checksum-type', range_headers)
                self.assertNotIn(self.CHECKSUM_FIELD, range_resp)
                self.assertNotIn('ChecksumType', range_resp)

                self.client.meta.events.register(
                    event_name, inject_invalid_checksum_mode)
                try:
                    for request_kwargs in ({}, {'Range': 'bytes=0-3'}):
                        with self.subTest(range=request_kwargs.get('Range')):
                            with self.assertRaises(
                                    botocore.exceptions.ClientError) as caught:
                                read_object(Bucket=self.bucket_name,
                                            Key=obj_name, **request_kwargs)
                            invalid_resp = caught.exception.response
                            self.assertEqual(400, invalid_resp[
                                'ResponseMetadata']['HTTPStatusCode'],
                                invalid_resp)
                            if operation == 'GetObject':
                                self.assertEqual('InvalidRequest',
                                                 invalid_resp['Error']['Code'],
                                                 invalid_resp)
                                self.assertEqual(
                                    'Value for x-amz-checksum-mode header '
                                    'is invalid.',
                                    invalid_resp['Error']['Message'],
                                    invalid_resp)
                            else:
                                self.assertEqual('400',
                                                 invalid_resp['Error']['Code'],
                                                 invalid_resp)
                finally:
                    self.client.meta.events.unregister(
                        event_name, inject_invalid_checksum_mode)

    def test_invalid_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-invalid')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=TEST_BODY,
                ChecksumAlgorithm=self.ALGORITHM,
                **{'Checksum' + self.ALGORITHM: self.INVALID}
            )
        self.assert_error(
            caught.exception.response,
            'InvalidRequest',
            'Value for %s header is invalid.' % self.CHECKSUM_HDR,
            obj_name,
        )

    def test_bad_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-bad')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=TEST_BODY,
                ChecksumAlgorithm=self.ALGORITHM,
                **{'Checksum' + self.ALGORITHM: self.BAD}
            )
        self.assert_error(
            caught.exception.response,
            'BadDigest',
            'The %s you specified did not match the calculated checksum.'
            % self.ALGORITHM,
            obj_name,
        )

    def test_mpu_upload_part_invalid_checksum(self):
        obj_name = self.create_name(
            self.ALGORITHM + '-mpu-upload-part-invalid-checksum')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm=self.ALGORITHM)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                **{'Checksum' + self.ALGORITHM: self.INVALID},
            )
        self.assert_error(
            caught.exception.response,
            'InvalidRequest',
            'Value for %s header is invalid.' % self.CHECKSUM_HDR,
            obj_name,
        )

    def test_mpu_upload_part_bad_checksum(self):
        obj_name = self.create_name(
            self.ALGORITHM + '-mpu-upload-part-bad-checksum')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm=self.ALGORITHM)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                **{'Checksum' + self.ALGORITHM: self.BAD},
            )
        self.assert_error(
            caught.exception.response,
            'BadDigest',
            'The %s you specified did not match the calculated '
            'checksum.' % self.ALGORITHM,
            obj_name,
        )

    def test_mpu_upload_part_good_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-mpu-upload-part-good')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm=self.ALGORITHM)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED},
        )
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

    def test_mpu_complete_good_checksum(self):
        checksum_kwargs = {
            'ChecksumAlgorithm': self.ALGORITHM,
        }
        if boto_at_least(1, 36):
            if self.ALGORITHM == 'CRC64NVME':
                # crc64nvme only allows full-object
                checksum_kwargs['ChecksumType'] = 'FULL_OBJECT'
            else:
                checksum_kwargs['ChecksumType'] = 'COMPOSITE'

        obj_name = self.create_name(self.ALGORITHM + '-mpu-complete-good')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            **checksum_kwargs)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED},
        )
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'ETag': part_resp['ETag'],
                        'PartNumber': 1,
                        'Checksum' + self.ALGORITHM: self.EXPECTED,
                    },
                ],
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])


class TestObjectChecksumCRC32(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'CRC32'
    CHECKSUM_HDR = 'x-amz-checksum-crc32'
    CHECKSUM_FIELD = 'ChecksumCRC32'
    EXPECTED = 'y/Q5Jg=='
    INVALID = 'y/Q5Jh=='
    BAD = 'z/Q5Jg=='


class TestObjectChecksumCRC32C(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'CRC32C'
    CHECKSUM_HDR = 'x-amz-checksum-crc32c'
    CHECKSUM_FIELD = 'ChecksumCRC32C'
    EXPECTED = '4waSgw=='
    INVALID = '4waSgx=='
    BAD = '5waSgw=='

    @classmethod
    def setUpClass(cls):
        if not botocore.httpchecksum.HAS_CRT:
            raise SkipTest('botocore cannot crc32c (run `pip install awscrt`)')
        super().setUpClass()


class TestObjectChecksumCRC64NVME(ObjectChecksumMixin,
                                  BaseS3TestCaseWithBucket):
    ALGORITHM = 'CRC64NVME'
    CHECKSUM_HDR = 'x-amz-checksum-crc64nvme'
    CHECKSUM_FIELD = 'ChecksumCRC64NVME'
    EXPECTED = 'rosUhgp5mIg='
    INVALID = 'rosUhgp5mIh='
    BAD = 'sosUhgp5mIg='

    @classmethod
    def setUpClass(cls):
        if [int(x) for x in botocore.__version__.split('.')] < [1, 36]:
            raise SkipTest('botocore cannot crc64nvme (run '
                           '`pip install -U boto3 botocore`)')
        if not botocore.httpchecksum.HAS_CRT:
            raise SkipTest('botocore cannot crc64nvme (run '
                           '`pip install awscrt`)')
        super().setUpClass()


class TestObjectChecksumSHA1(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'SHA1'
    CHECKSUM_HDR = 'x-amz-checksum-sha1'
    CHECKSUM_FIELD = 'ChecksumSHA1'
    EXPECTED = '98O8HYCOBHMq32eZZczDTKeuNEE='
    INVALID = '98O8HYCOBHMq32eZZczDTKeuNEF='
    BAD = '+8O8HYCOBHMq32eZZczDTKeuNEE='


class TestObjectChecksumSHA256(ObjectChecksumMixin, BaseS3TestCaseWithBucket):
    ALGORITHM = 'SHA256'
    CHECKSUM_HDR = 'x-amz-checksum-sha256'
    CHECKSUM_FIELD = 'ChecksumSHA256'
    EXPECTED = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='
    INVALID = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiV='
    BAD = 'GeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='


class TestObjectChecksums(BaseS3TestCaseWithBucket):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

    def test_multi_checksum(self):
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=self.create_name('multi-checksum'),
                Body=TEST_BODY,
                # Note: Both valid! Ought to be able to validate & store both
                ChecksumCRC32='y/Q5Jg==',
                ChecksumSHA1='98O8HYCOBHMq32eZZczDTKeuNEE=',
            )
        resp = caught.exception.response
        code = resp['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(400, code)
        self.assertEqual('InvalidRequest', resp['Error']['Code'])
        self.assertEqual(
            resp['Error']['Message'],
            'Expecting a single x-amz-checksum- header. '
            'Multiple checksum Types are not allowed.')

    @skip_if_checksum_persistence_disabled
    def test_empty_full_object_checksum_persistence(self):
        obj_name = self.create_name('empty-full-object')
        expected = base64_str(crc32(b'').digest())
        sent_headers = {}
        capture = capture_prepared_headers(sent_headers)
        event_name = 'before-send.s3.PutObject'
        self.client.meta.events.register(event_name, capture)
        try:
            put_resp = self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=b'',
                ChecksumCRC32=expected)
        finally:
            self.client.meta.events.unregister(event_name, capture)

        self.assertEqual('0', sent_headers.get('content-length'))
        self.assertNotIn('transfer-encoding', sent_headers)
        self.assertEqual(
            200, put_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(expected, put_resp['ChecksumCRC32'])
        self.assertEqual('FULL_OBJECT', put_resp['ChecksumType'])

        head_resp = self.client.head_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            ChecksumMode='ENABLED')
        self.assertEqual(
            200, head_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(0, head_resp['ContentLength'])
        self.assertEqual(expected, head_resp['ChecksumCRC32'])
        self.assertEqual('FULL_OBJECT', head_resp['ChecksumType'])

        get_resp = self.client.get_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            ChecksumMode='ENABLED')
        self.assertEqual(
            200, get_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(b'', get_resp['Body'].read())
        self.assertEqual(expected, get_resp['ChecksumCRC32'])
        self.assertEqual('FULL_OBJECT', get_resp['ChecksumType'])

    def test_different_checksum_requested(self):
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=self.create_name('different-checksum'),
                Body=TEST_BODY,
                ChecksumCRC32='y/Q5Jg==',
                ChecksumAlgorithm='SHA1',
            )
        resp = caught.exception.response
        code = resp['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(400, code)
        self.assertEqual('InvalidRequest', resp['Error']['Code'])
        if boto_at_least(1, 36):
            self.assertEqual(
                resp['Error']['Message'],
                'Value for x-amz-sdk-checksum-algorithm header is invalid.')
        else:
            self.assertEqual(
                resp['Error']['Message'],
                'Expecting a single x-amz-checksum- header')

    def assert_invalid(self, resp):
        code = resp['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(400, code)
        self.assertEqual('InvalidRequest', resp['Error']['Code'])
        self.assertEqual(
            resp['Error']['Message'],
            'Value for x-amz-checksum-crc32 header is invalid.')

    def test_invalid_base64_invalid_length(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('invalid-bad-length'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'short===',  # invalid length for base64
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_too_short(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('invalid-short'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'shrt',  # only 3 bytes
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_too_long(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('invalid-long'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'toolong=',  # 5 bytes
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_all_invalid_chars(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('purely-invalid'),
            'Body': TEST_BODY,
            'ChecksumCRC32': '^^^^^^==',  # all invalid char
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_invalid_base64_includes_invalid_chars(self):
        put_kwargs = {
            'Bucket': self.bucket_name,
            'Key': self.create_name('contains-invalid'),
            'Body': TEST_BODY,
            'ChecksumCRC32': 'y^/^Q5^J^g==',  # spaced out with invalid chars
        }
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(**put_kwargs)
        self.assert_invalid(caught.exception.response)

    def test_mpu_no_checksum_upload_part_invalid_checksum(self):
        obj_name = self.create_name('no-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                ChecksumCRC32=TestObjectChecksumCRC32.INVALID,
            )
        self.assert_invalid(caught.exception.response)

    def test_mpu_has_no_checksum(self):
        # Clients don't need to be thinking about checksums at all
        obj_name = self.create_name('no-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name)
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=TEST_BODY,
        )
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'ETag': part_resp['ETag'],
                        'PartNumber': 1,
                    },
                ],
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        head_resp = self.client.head_object(
            Bucket=self.bucket_name, Key=obj_name)
        self.assertFalse([k for k in head_resp
                          if k.startswith('Checksum')])

    def test_mpu_upload_part_multi_checksum(self):
        obj_name = self.create_name('multi-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32C')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                Body=TEST_BODY,
                # Both valid!
                ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED,
                ChecksumCRC32C=TestObjectChecksumCRC32C.EXPECTED,
            )
        resp = caught.exception.response
        self.assertEqual(400, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Error'], {
            'Code': 'InvalidRequest',
            'Message': ('Expecting a single x-amz-checksum- header. '
                        'Multiple checksum Types are not allowed.'),
        })
        # You'd think we ought to be able to validate & store both...

    def test_multipart_mpu(self):
        obj_name = self.create_name('multipart-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32C')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_body = b'\x00' * 5 * 1024 * 1024
        part_crc32c = base64_str(crc32c(part_body).digest())

        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        # then do another
        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=2,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': 1,
                        'ETag': upload_part_resp['ETag'],
                        'ChecksumCRC32C': part_crc32c,
                    },
                    {
                        'PartNumber': 2,
                        'ETag': upload_part_resp['ETag'],
                        'ChecksumCRC32C': part_crc32c,
                    },
                ],
            },
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        mpu_etag = '"' + hashlib.md5(binascii.unhexlify(
            upload_part_resp['ETag'].strip('"')) * 2).hexdigest() + '-2"'
        self.assertEqual(mpu_etag,
                         complete_mpu_resp['ETag'])

    def test_multipart_mpu_no_etags(self):
        obj_name = self.create_name('multipart-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32C')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_body = b'\x00' * 5 * 1024 * 1024
        part_crc32c = base64_str(crc32c(part_body).digest())

        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        # then do another
        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=2,
            Body=part_body,
            ChecksumCRC32C=part_crc32c,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={
                    'Parts': [
                        {
                            'PartNumber': 1,
                            'ChecksumCRC32C': part_crc32c,
                        },
                        {
                            'PartNumber': 2,
                            'ChecksumCRC32C': part_crc32c,
                        },
                    ],
                },
                UploadId=upload_id,
            )
        resp = caught.exception.response
        self.assertEqual(400, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(resp['Error']['Code'], 'MalformedXML')
        self.assertEqual(
            resp['Error']['Message'],
            'The XML you provided was not well-formed or did not validate '
            'against our published schema'
        )
        abort_resp = self.client.abort_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            UploadId=upload_id,
        )
        self.assertEqual(204, abort_resp[
            'ResponseMetadata']['HTTPStatusCode'])

    def test_multipart_mpu_composite_checksum(self):
        obj_name = self.create_name('composite-checksum-mpu')
        create_mpu_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32', ChecksumType='COMPOSITE')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        upload_id = create_mpu_resp['UploadId']
        part_body = b'\x00' * 5 * 1024 * 1024
        part_crc32 = base64_str(crc32(part_body).digest())

        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=part_body,
            ChecksumCRC32=part_crc32,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        # then do another
        upload_part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=2,
            Body=part_body,
            ChecksumCRC32=part_crc32,
        )
        self.assertEqual(200, upload_part_resp[
            'ResponseMetadata']['HTTPStatusCode'])

        # AWS accepts `<b64digest>-<count>`: the crc32 of the concatenated
        # raw part digests, then the part count
        composite_checksum = '%s-2' % base64_str(
            crc32(binascii.a2b_base64(part_crc32) * 2).digest())

        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={
                'Parts': [
                    {
                        'PartNumber': 1,
                        'ETag': upload_part_resp['ETag'],
                        'ChecksumCRC32': part_crc32,
                    },
                    {
                        'PartNumber': 2,
                        'ETag': upload_part_resp['ETag'],
                        'ChecksumCRC32': part_crc32,
                    },
                ],
            },
            UploadId=upload_id,
            ChecksumCRC32=composite_checksum,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
