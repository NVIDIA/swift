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
import contextlib
import hashlib
from unittest import SkipTest

from swift.common.utils import base64_str
from swift.common.utils.checksum import crc32, crc32c
from test.s3api import (BaseS3TestCaseWithBucket, code_from_error,
                        skip_if_checksum_persistence_disabled)

TEST_BODY = b'123456789'

# S3 requires every MPU part except the final one to be at least 5 MiB.
MIN_MPU_PART_SIZE = 5 * 2 ** 20
MPU_PART_BODIES = (
    b'a' * MIN_MPU_PART_SIZE,
    b'last part',
)


def _crc32(body):
    """Create the base64 CRC32 digest S3 expects for a body."""
    return base64_str(crc32(body).digest())


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


class BaseS3TestCaseObjectChecksum(BaseS3TestCaseWithBucket):

    @contextlib.contextmanager
    def disable_client_checksum_calculation(self):
        # Temporarily configure client so boto does not automatically send
        # checksums on our behalf
        previous_request_checksum_calculation = getattr(
            self.client.meta.config, 'request_checksum_calculation', None)
        self.client.meta.config.request_checksum_calculation = 'when_required'
        try:
            yield
        finally:
            self.client.meta.config.request_checksum_calculation = \
                previous_request_checksum_calculation

    def assert_error(
            self, resp, err_code, err_msg=None, obj_name=None,
            status_code=400, **extra):
        """Assert the expected fields in an S3 error response."""
        self.assertEqual(
            status_code, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(err_code, resp['Error']['Code'])
        if err_msg is not None:
            self.assertEqual(err_msg, resp['Error']['Message'])
        self.assertEqual({k: resp['Error'].get(k) for k in extra}, extra)

        if obj_name is not None:
            # Sanity check: object was not created
            with self.assertRaises(botocore.exceptions.ClientError) as caught:
                self.client.head_object(Bucket=self.bucket_name, Key=obj_name)
            self.assert_error(
                resp=caught.exception.response,
                err_code='404',
                err_msg='Not Found',
                status_code=404)

    def _assert_checksum_response(self, resp):
        """Check a successful response and its checksum headers."""
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        headers = resp['ResponseMetadata']['HTTPHeaders']
        self.assertEqual(self.EXPECTED, headers.get(self.CHECKSUM_HDR))
        self.assertEqual('FULL_OBJECT', headers.get('x-amz-checksum-type'))
        self.assertEqual(self.EXPECTED, resp[self.CHECKSUM_FIELD])
        self.assertEqual('FULL_OBJECT', resp['ChecksumType'])

    def _abort_mpu_if_pending(self, obj_name, upload_id):
        try:
            self.client.abort_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id)
        except botocore.exceptions.ClientError as err:
            # An already-completed or already-aborted upload is gone; AWS
            # reports that as NoSuchUpload, Swift as NoSuchKey.
            if code_from_error(err) not in ('NoSuchUpload', 'NoSuchKey'):
                raise

    def create_mpu(self, obj_name, algorithm=None, checksum_type=None,
                   **kwargs):
        """Create an MPU, assert its checksum settings, return upload id."""
        if algorithm is not None:
            kwargs['ChecksumAlgorithm'] = algorithm
        if checksum_type is not None:
            kwargs['ChecksumType'] = checksum_type
        resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name, **kwargs)
        upload_id = resp['UploadId']
        self.addCleanup(self._abort_mpu_if_pending, obj_name, upload_id)
        if algorithm is None:
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        else:
            self._assert_checksum_create_mpu(
                create_mpu_resp=resp, upload_id=upload_id,
                checksum_algorithm=algorithm, checksum_type=checksum_type)
        return upload_id

    def upload_part(self, obj_name, upload_id, part_number, body, **kwargs):
        """Upload a part, assert its checksum, return its manifest entry."""
        resp = self.client.upload_part(
            Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id,
            PartNumber=part_number, Body=body, **kwargs)
        part = {'ETag': resp['ETag'], 'PartNumber': part_number}
        algorithm = kwargs.get('ChecksumAlgorithm')
        if algorithm:
            checksum_field = 'Checksum' + algorithm
        else:
            checksum_field = next(
                (k for k in kwargs if k.startswith('Checksum')), None)
        if checksum_field is None:
            self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
            return part
        if checksum_field in kwargs:
            part_checksum = kwargs[checksum_field]
        else:
            part_checksum = resp[checksum_field]
        self._assert_checksum_upload_part_mpu(
            part_resp=resp,
            part_checksum=part_checksum,
            checksum_header='x-amz-checksum-%s' % checksum_field[
                len('Checksum'):].lower(),
            checksum_field=checksum_field)
        part[checksum_field] = part_checksum
        return part

    def _assert_checksum_create_mpu(
            self, create_mpu_resp, upload_id, checksum_algorithm,
            checksum_type=None):
        """Assert CreateMultipartUpload returned its checksum settings."""
        if checksum_type is None:
            checksum_type = (
                'FULL_OBJECT' if checksum_algorithm == 'CRC64NVME'
                else 'COMPOSITE')
        self.assertEqual(200, create_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        headers = create_mpu_resp['ResponseMetadata']['HTTPHeaders']
        self.assertEqual(
            checksum_algorithm,
            headers.get('x-amz-checksum-algorithm'))
        self.assertEqual(
            checksum_type, headers.get('x-amz-checksum-type'))
        self.assertEqual(
            checksum_algorithm, create_mpu_resp['ChecksumAlgorithm'])
        self.assertEqual(checksum_type, create_mpu_resp['ChecksumType'])
        self.assertEqual(upload_id, create_mpu_resp['UploadId'])

    def _assert_checksum_upload_part_mpu(
            self, part_resp, part_checksum, checksum_header,
            checksum_field):
        """Assert UploadPart returned only its calculated checksum."""
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        headers = part_resp['ResponseMetadata']['HTTPHeaders']
        self.assertEqual(part_checksum, headers.get(checksum_header))
        self.assertNotIn('x-amz-checksum-type', headers)
        self.assertEqual(part_checksum, part_resp[checksum_field])
        self.assertNotIn('ChecksumType', part_resp)


class ObjectChecksumMixin(object):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

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
        # Use a legacy MPU to test part validation with writes on or off.
        upload_id = self.create_mpu(obj_name)
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
        # Use a legacy MPU to test part validation with writes on or off.
        upload_id = self.create_mpu(obj_name)
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

    @skip_if_checksum_persistence_disabled
    def test_mpu_upload_part_good_checksum(self):
        obj_name = self.create_name(self.ALGORITHM + '-mpu-upload-part-good')
        upload_id = self.create_mpu(obj_name, algorithm=self.ALGORITHM)
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
        self.assertEqual(
            self.EXPECTED, part_resp['Checksum' + self.ALGORITHM])
        self.assertNotIn('ChecksumType', part_resp)

    @skip_if_checksum_persistence_disabled
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
        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            **{'Checksum' + self.ALGORITHM: self.EXPECTED})
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={'Parts': [part]},
            UploadId=upload_id,
        )
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])


class TestObjectChecksumCRC32(ObjectChecksumMixin,
                              BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'CRC32'
    CHECKSUM_HDR = 'x-amz-checksum-crc32'
    CHECKSUM_FIELD = 'ChecksumCRC32'
    EXPECTED = 'y/Q5Jg=='
    INVALID = 'y/Q5Jh=='
    BAD = 'z/Q5Jg=='


class TestObjectChecksumCRC32C(ObjectChecksumMixin,
                               BaseS3TestCaseObjectChecksum):
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


class TestObjectChecksumCRC64NVME(
        ObjectChecksumMixin, BaseS3TestCaseObjectChecksum):
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


class TestObjectChecksumSHA1(ObjectChecksumMixin,
                             BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'SHA1'
    CHECKSUM_HDR = 'x-amz-checksum-sha1'
    CHECKSUM_FIELD = 'ChecksumSHA1'
    EXPECTED = '98O8HYCOBHMq32eZZczDTKeuNEE='
    INVALID = '98O8HYCOBHMq32eZZczDTKeuNEF='
    BAD = '+8O8HYCOBHMq32eZZczDTKeuNEE='


class TestObjectChecksumSHA256(ObjectChecksumMixin,
                               BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'SHA256'
    CHECKSUM_HDR = 'x-amz-checksum-sha256'
    CHECKSUM_FIELD = 'ChecksumSHA256'
    EXPECTED = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='
    INVALID = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiV='
    BAD = 'GeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='


class TestObjectChecksums(BaseS3TestCaseObjectChecksum):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

    def test_multi_checksum(self):
        obj_name = self.create_name('multi-checksum')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=TEST_BODY,
                # Note: Both valid! Ought to be able to validate & store both
                ChecksumCRC32='y/Q5Jg==',
                ChecksumSHA1='98O8HYCOBHMq32eZZczDTKeuNEE=',
            )
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg=('Expecting a single x-amz-checksum- header. '
                     'Multiple checksum Types are not allowed.'),
            obj_name=obj_name)

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
        obj_name = self.create_name('different-checksum')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                Body=TEST_BODY,
                ChecksumCRC32='y/Q5Jg==',
                ChecksumAlgorithm='SHA1',
            )
        if boto_at_least(1, 36):
            err_msg = (
                'Value for x-amz-sdk-checksum-algorithm header is invalid.')
        else:
            err_msg = 'Expecting a single x-amz-checksum- header'
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg=err_msg,
            obj_name=obj_name)

    def test_invalid_base64(self):
        for scenario, value in (
                ('bad-length', 'short==='),   # Invalid base64 length.
                ('short', 'shrt'),            # Only three decoded bytes.
                ('long', 'toolong='),         # Five decoded bytes.
                ('all-invalid', '^^^^^^=='),  # all invalid characters
                ('contains-invalid', 'y^/^Q5^J^g==')):
            with self.subTest(scenario=scenario):
                obj_name = self.create_name('invalid-' + scenario)
                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.put_object(
                        Bucket=self.bucket_name, Key=obj_name, Body=TEST_BODY,
                        ChecksumCRC32=value)
                self.assert_error(
                    resp=caught.exception.response,
                    err_code='InvalidRequest',
                    err_msg=(
                        'Value for x-amz-checksum-crc32 header is invalid.'),
                    obj_name=obj_name)

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

    @skip_if_checksum_persistence_disabled
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

    @skip_if_checksum_persistence_disabled
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

    @skip_if_checksum_persistence_disabled
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


class TestObjectChecksumsMPUs(BaseS3TestCaseObjectChecksum):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

    def test_no_checksum_upload_part_checksum_not_echoed(self):
        obj_name = self.create_name('no-checksum-mpu-part-checksum')
        upload_id = self.create_mpu(obj_name)

        # Supplying a checksum must not enroll a legacy MPU.
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id,
            PartNumber=1, Body=TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('ChecksumCRC32', part_resp)
        self.assertNotIn('ChecksumType', part_resp)
        headers = part_resp['ResponseMetadata']['HTTPHeaders']
        self.assertNotIn('x-amz-checksum-crc32', headers)
        self.assertNotIn('x-amz-checksum-type', headers)

    @skip_if_checksum_persistence_disabled
    def test_create_checksum_type_requires_algorithm(self):
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=self.create_name('mpu-checksum-type-only'),
                ChecksumType='FULL_OBJECT')
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg=(
                'The x-amz-checksum-type header can only be used with the '
                'x-amz-checksum-algorithm header.'))

    @skip_if_checksum_persistence_disabled
    def test_create_rejects_unknown_checksum_algorithm(self):
        # S3 matches the algorithm name case-insensitively but does not accept
        # hyphenated spellings, so 'SHA-256' is as unsupported as 'invalid'.
        for algorithm in ('invalid', 'SHA-1', 'SHA-256', 'sha-256'):
            with self.subTest(algorithm=algorithm):
                obj_name = self.create_name(
                    'mpu-unknown-checksum-algorithm')
                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.create_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        ChecksumAlgorithm=algorithm)

                resp = caught.exception.response
                self.assert_error(
                    resp=resp,
                    err_code='InvalidRequest',
                    obj_name=obj_name)

                # Since we don't support all the algorithms yet,
                # we can't do a basic string comparision
                message_prefix = (
                    'Checksum algorithm provided is unsupported. Please try '
                    'again with any of the valid types: [')
                message = resp['Error']['Message']
                self.assertTrue(message.startswith(message_prefix), resp)
                self.assertTrue(message.endswith(']'), resp)
                supported_algorithms = message[
                    len(message_prefix):-1].split(', ')
                for checksum_algorithm in (
                        'CRC32', 'CRC32C', 'CRC64NVME', 'SHA1', 'SHA256'):
                    self.assertIn(checksum_algorithm, supported_algorithms)

    @skip_if_checksum_persistence_disabled
    def test_create_rejects_invalid_checksum_type(self):
        # S3 validates the type before the algorithm.
        # Test both a bad type and an empty type.
        for checksum_params in (
                {'ChecksumAlgorithm': 'CRC32', 'ChecksumType': 'cheese fries'},
                {'ChecksumType': 'cheese fries'},
                {'ChecksumAlgorithm': 'CRC32', 'ChecksumType': ''}):
            with self.subTest(checksum_params=checksum_params):
                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.create_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=self.create_name('mpu-invalid-checksum-type'),
                        **checksum_params)
                self.assert_error(
                    resp=caught.exception.response,
                    err_code='InvalidRequest',
                    err_msg='Value for x-amz-checksum-type header is invalid.')

    @skip_if_checksum_persistence_disabled
    def test_create_normalizes_checksum_type(self):
        for supplied_checksum_type, expected_checksum_type in (
                ('fUll_ObJEct', 'FULL_OBJECT'),
                ('ComPosIte', 'COMPOSITE')):
            with self.subTest(checksum_type=supplied_checksum_type):
                obj_name = self.create_name(
                    'mpu-normalize-' + supplied_checksum_type.lower())
                create_mpu_resp = self.client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=obj_name,
                    ChecksumAlgorithm='CRC32',
                    ChecksumType=supplied_checksum_type)
                upload_id = create_mpu_resp['UploadId']
                try:
                    self._assert_checksum_create_mpu(
                        create_mpu_resp=create_mpu_resp,
                        upload_id=upload_id,
                        checksum_algorithm='CRC32',
                        checksum_type=expected_checksum_type)
                finally:
                    self.client.abort_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        UploadId=upload_id)

    @skip_if_checksum_persistence_disabled
    def test_create_rejects_algorithm_type_mismatch(self):
        # FULL_OBJECT requires CRC. CRC64NVME does not support COMPOSITE.
        for algorithm, checksum_type in (('SHA1', 'FULL_OBJECT'),
                                         ('CRC64NVME', 'COMPOSITE')):
            with self.subTest(algorithm=algorithm,
                              checksum_type=checksum_type):
                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.create_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=self.create_name('mpu-algorithm-type-mismatch'),
                        ChecksumAlgorithm=algorithm,
                        ChecksumType=checksum_type)
                self.assert_error(
                    resp=caught.exception.response,
                    err_code='InvalidRequest',
                    err_msg=(
                        'The %s checksum type cannot be used with the %s '
                        'checksum algorithm.' % (
                            checksum_type, algorithm.lower())))

    @skip_if_checksum_persistence_disabled
    def test_upload_part_rejects_checksum_algorithm_mismatch(self):
        for checksum_type in ('COMPOSITE', 'FULL_OBJECT'):
            with self.subTest(checksum_type=checksum_type):
                obj_name = self.create_name(
                    'mpu-upload-checksum-algorithm-mismatch')
                upload_id = self.create_mpu(obj_name, 'CRC32', checksum_type)

                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.upload_part(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        UploadId=upload_id,
                        PartNumber=1,
                        Body=TEST_BODY,
                        ChecksumSHA256=TestObjectChecksumSHA256.EXPECTED)
                self.assert_error(
                    resp=caught.exception.response,
                    err_code='InvalidRequest',
                    err_msg=(
                        'Checksum Type mismatch occurred, expected checksum '
                        'Type: crc32, actual checksum Type: sha256'))

    @skip_if_checksum_persistence_disabled
    def test_composite_upload_part_requires_checksum(self):
        obj_name = self.create_name(
            'mpu-composite-upload-part-missing-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')
        with self.disable_client_checksum_calculation():
            with self.assertRaises(botocore.exceptions.ClientError) as caught:
                self.client.upload_part(
                    Bucket=self.bucket_name,
                    Key=obj_name,
                    UploadId=upload_id,
                    PartNumber=1,
                    # We need to send a checksum here
                    Body=TEST_BODY)
            self.assert_error(
                resp=caught.exception.response,
                err_code='InvalidRequest',
                err_msg=(
                    'Checksum Type mismatch occurred, expected checksum '
                    'Type: crc32, actual checksum Type: null'))

    @skip_if_checksum_persistence_disabled
    def test_full_object_upload_part_checksum_optional(self):
        obj_name = self.create_name(
            'mpu-full-object-upload-part-checksum-optional')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')
        with self.disable_client_checksum_calculation():
            body = MPU_PART_BODIES[0]
            part_resp = self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=1,
                # No part checksum; the server calculates it for us
                Body=body)
            self._assert_checksum_upload_part_mpu(
                part_resp=part_resp,
                part_checksum=_crc32(body),
                checksum_header='x-amz-checksum-crc32',
                checksum_field='ChecksumCRC32')
