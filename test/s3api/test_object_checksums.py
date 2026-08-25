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
from tempfile import NamedTemporaryFile
from unittest import SkipTest

from boto3.s3.transfer import TransferConfig

from swift.common.utils import base64_str
from swift.common.utils.checksum import crc32, crc32c, crc64nvme
from test.s3api import (BaseS3TestCaseWithBucket, code_from_error,
                        get_cluster_info,
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


def make_composite_checksum(checksum_hasher_constructor, part_bodies):
    """Create an S3 composite checksum from ordered part bodies."""
    part_bodies = tuple(part_bodies)
    part_checksum_values = (
        checksum_hasher_constructor(part_body).digest()
        for part_body in part_bodies)
    return '%s-%d' % (
        base64_str(checksum_hasher_constructor(
            b''.join(part_checksum_values)).digest()),
        len(part_bodies))


def make_full_object_checksum(checksum_hasher_constructor, part_bodies):
    """Create an S3 full-object checksum from ordered part bodies."""
    return base64_str(
        checksum_hasher_constructor(b''.join(part_bodies)).digest())


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

    def _assert_checksum_complete_mpu(
            self, complete_mpu_resp, expected_checksum, checksum_field,
            checksum_type):
        """Assert CompleteMultipartUpload returned the final checksum."""
        self.assertEqual(200, complete_mpu_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            expected_checksum, complete_mpu_resp[checksum_field])
        self.assertEqual(checksum_type, complete_mpu_resp['ChecksumType'])

    def _assert_mpu_checksum_persistence(
            self, obj_name, expected_body, checksum_algorithm,
            expected_checksum, checksum_type):
        """Assert HEAD and GET return an object's persisted checksum."""
        checksum_header = 'x-amz-checksum-' + checksum_algorithm.lower()
        checksum_field = 'Checksum' + checksum_algorithm

        head_resp = self.client.head_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            ChecksumMode='ENABLED')
        self.assertEqual(
            200, head_resp['ResponseMetadata']['HTTPStatusCode'])
        head_headers = head_resp['ResponseMetadata']['HTTPHeaders']
        self.assertEqual(
            expected_checksum, head_headers.get(checksum_header))
        self.assertEqual(
            checksum_type, head_headers.get('x-amz-checksum-type'))
        self.assertEqual(expected_checksum, head_resp[checksum_field])
        self.assertEqual(checksum_type, head_resp['ChecksumType'])

        get_resp = self.client.get_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            ChecksumMode='ENABLED')
        self.assertEqual(
            200, get_resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(expected_body, get_resp['Body'].read())
        get_headers = get_resp['ResponseMetadata']['HTTPHeaders']
        self.assertEqual(
            expected_checksum, get_headers.get(checksum_header))
        self.assertEqual(
            checksum_type, get_headers.get('x-amz-checksum-type'))
        self.assertEqual(expected_checksum, get_resp[checksum_field])
        self.assertEqual(checksum_type, get_resp['ChecksumType'])


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
        checksum_type = None
        if boto_at_least(1, 36):
            if self.ALGORITHM == 'CRC64NVME':
                # crc64nvme only allows full-object
                checksum_type = 'FULL_OBJECT'
            else:
                checksum_type = 'COMPOSITE'

        obj_name = self.create_name(self.ALGORITHM + '-mpu-complete-good')
        upload_id = self.create_mpu(
            obj_name, algorithm=self.ALGORITHM, checksum_type=checksum_type)
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


class BaseObjectChecksumMPUMixin(object):
    def _test_mpu_checksum_round_trip(self, checksum_type):
        self.obj_name = self.create_name(
            self.ALGORITHM.lower() + '-mpu-' + checksum_type.lower())
        # CreateMultiPartUpload
        self.upload_id = self.create_mpu(
            self.obj_name, self.ALGORITHM, checksum_type)
        parts = []
        part_numbers = range(1, len(MPU_PART_BODIES) + 1)
        # UploadPart
        for part_number, body in zip(
                part_numbers, MPU_PART_BODIES):
            upload_part_kwargs = {}
            if checksum_type == 'COMPOSITE':
                upload_part_kwargs['ChecksumAlgorithm'] = self.ALGORITHM
            else:
                upload_part_kwargs[self.CHECKSUM_FIELD] = base64_str(
                    self.CHECKSUM_HASHER_CONSTRUCTOR(body).digest())
            parts.append(self.upload_part(
                self.obj_name, self.upload_id, part_number, body,
                **upload_part_kwargs))

        expected_body = b''.join(MPU_PART_BODIES)
        if checksum_type == 'COMPOSITE':
            expected_checksum = make_composite_checksum(
                checksum_hasher_constructor=self.CHECKSUM_HASHER_CONSTRUCTOR,
                part_bodies=MPU_PART_BODIES)
        else:
            expected_checksum = make_full_object_checksum(
                checksum_hasher_constructor=self.CHECKSUM_HASHER_CONSTRUCTOR,
                part_bodies=MPU_PART_BODIES)
        complete_kwargs = {
            'ChecksumType': checksum_type,
            self.CHECKSUM_FIELD: expected_checksum,
        }

        # CompleteMultiPartUpload
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=self.obj_name,
            MultipartUpload={'Parts': parts},
            UploadId=self.upload_id,
            **complete_kwargs)
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_mpu_resp,
            expected_checksum=expected_checksum,
            checksum_field=self.CHECKSUM_FIELD,
            checksum_type=checksum_type)

        self._assert_mpu_checksum_persistence(
            obj_name=self.obj_name,
            expected_body=expected_body,
            checksum_algorithm=self.ALGORITHM,
            expected_checksum=expected_checksum,
            checksum_type=checksum_type)

    def _test_mpu_checksum_single_zero_length_part(self, checksum_type):
        self.obj_name = self.create_name(
            self.ALGORITHM.lower() + '-mpu-' + checksum_type.lower())
        # CreateMultiPartUpload
        self.upload_id = self.create_mpu(
            self.obj_name, self.ALGORITHM, checksum_type)
        empty_checksum = base64_str(
            self.CHECKSUM_HASHER_CONSTRUCTOR(b'').digest())
        if checksum_type == 'COMPOSITE':
            expected_checksum = make_composite_checksum(
                checksum_hasher_constructor=self.CHECKSUM_HASHER_CONSTRUCTOR,
                part_bodies=(b'',))
        else:
            expected_checksum = empty_checksum
        complete_kwargs = {
            'ChecksumType': checksum_type,
            self.CHECKSUM_FIELD: expected_checksum,
        }

        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=self.obj_name,
            UploadId=self.upload_id,
            PartNumber=1,
            Body=b'',
            **{self.CHECKSUM_FIELD: empty_checksum})
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            empty_checksum, part_resp[self.CHECKSUM_FIELD])

        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.obj_name,
            UploadId=self.upload_id,
            MultipartUpload={
                'Parts': [{
                    'ETag': part_resp['ETag'],
                    'PartNumber': 1,
                    self.CHECKSUM_FIELD: empty_checksum,
                }],
            },
            **complete_kwargs)
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field=self.CHECKSUM_FIELD,
            checksum_type=checksum_type)

        self._assert_mpu_checksum_persistence(
            obj_name=self.obj_name,
            expected_body=b'',
            checksum_algorithm=self.ALGORITHM,
            expected_checksum=expected_checksum,
            checksum_type=checksum_type)


class ObjectChecksumMPUCompositeMixin(BaseObjectChecksumMPUMixin):
    @skip_if_checksum_persistence_disabled
    def test_mpu_composite_checksum_round_trip(self):
        self._test_mpu_checksum_round_trip('COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_mpu_composite_checksum_single_zero_length_part(self):
        self._test_mpu_checksum_single_zero_length_part('COMPOSITE')


class ObjectChecksumMPUFullObjectMixin(BaseObjectChecksumMPUMixin):
    @skip_if_checksum_persistence_disabled
    def test_mpu_full_object_checksum_round_trip(self):
        self._test_mpu_checksum_round_trip('FULL_OBJECT')

    @skip_if_checksum_persistence_disabled
    def test_mpu_full_object_checksum_single_zero_length_part(self):
        self._test_mpu_checksum_single_zero_length_part('FULL_OBJECT')


class TestObjectChecksumCRC32(ObjectChecksumMixin,
                              ObjectChecksumMPUCompositeMixin,
                              ObjectChecksumMPUFullObjectMixin,
                              BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'CRC32'
    CHECKSUM_HDR = 'x-amz-checksum-crc32'
    CHECKSUM_FIELD = 'ChecksumCRC32'
    EXPECTED = 'y/Q5Jg=='
    INVALID = 'y/Q5Jh=='
    BAD = 'z/Q5Jg=='
    CHECKSUM_HASHER_CONSTRUCTOR = staticmethod(crc32)


class TestObjectChecksumCRC32C(ObjectChecksumMixin,
                               ObjectChecksumMPUCompositeMixin,
                               ObjectChecksumMPUFullObjectMixin,
                               BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'CRC32C'
    CHECKSUM_HDR = 'x-amz-checksum-crc32c'
    CHECKSUM_FIELD = 'ChecksumCRC32C'
    EXPECTED = '4waSgw=='
    INVALID = '4waSgx=='
    BAD = '5waSgw=='
    CHECKSUM_HASHER_CONSTRUCTOR = staticmethod(crc32c)

    @classmethod
    def setUpClass(cls):
        if not botocore.httpchecksum.HAS_CRT:
            raise SkipTest('botocore cannot crc32c (run `pip install awscrt`)')
        super().setUpClass()


class TestObjectChecksumCRC64NVME(ObjectChecksumMixin,
                                  ObjectChecksumMPUFullObjectMixin,
                                  BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'CRC64NVME'
    CHECKSUM_HDR = 'x-amz-checksum-crc64nvme'
    CHECKSUM_FIELD = 'ChecksumCRC64NVME'
    EXPECTED = 'rosUhgp5mIg='
    INVALID = 'rosUhgp5mIh='
    BAD = 'sosUhgp5mIg='
    CHECKSUM_HASHER_CONSTRUCTOR = staticmethod(crc64nvme)

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
                             ObjectChecksumMPUCompositeMixin,
                             BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'SHA1'
    CHECKSUM_HDR = 'x-amz-checksum-sha1'
    CHECKSUM_FIELD = 'ChecksumSHA1'
    EXPECTED = '98O8HYCOBHMq32eZZczDTKeuNEE='
    INVALID = '98O8HYCOBHMq32eZZczDTKeuNEF='
    BAD = '+8O8HYCOBHMq32eZZczDTKeuNEE='
    CHECKSUM_HASHER_CONSTRUCTOR = staticmethod(hashlib.sha1)


class TestObjectChecksumSHA256(ObjectChecksumMixin,
                               ObjectChecksumMPUCompositeMixin,
                               BaseS3TestCaseObjectChecksum):
    ALGORITHM = 'SHA256'
    CHECKSUM_HDR = 'x-amz-checksum-sha256'
    CHECKSUM_FIELD = 'ChecksumSHA256'
    EXPECTED = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='
    INVALID = 'FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiV='
    BAD = 'GeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU='
    CHECKSUM_HASHER_CONSTRUCTOR = staticmethod(hashlib.sha256)


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

        # Upload equal parts so both entries use the same ETag.
        for part_number in (1, 2):
            upload_part_resp = self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=part_number,
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
        upload_id = self.create_mpu(obj_name)
        part_body = b'\x00' * 5 * 1024 * 1024
        part_crc32c = base64_str(crc32c(part_body).digest())

        # Upload equal parts so both entries use the same ETag.
        for part_number in (1, 2):
            upload_part_resp = self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=part_number,
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


class TestObjectChecksumsMPUs(BaseS3TestCaseObjectChecksum):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.client = cls.get_s3_client(1)
        cls.use_tls = cls.client._endpoint.host.startswith('https:')

    def test_complete_checksum_request_persistence_disabled(self):
        s3api_info = get_cluster_info().get('s3api')
        if (s3api_info is None or
                s3api_info.get('allow_store_checksums', False)):
            self.skipTest('This test requires allow_store_checksums=false')

        obj_name = self.create_name('mpu-checksum-request-writes-disabled')
        create_resp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            ChecksumAlgorithm='CRC32', ChecksumType='COMPOSITE')
        self.assertEqual(200, create_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('ChecksumAlgorithm', create_resp)
        self.assertNotIn('ChecksumType', create_resp)
        upload_id = create_resp['UploadId']
        self.addCleanup(self._abort_mpu_if_pending, obj_name, upload_id)

        part_checksum = _crc32(TEST_BODY)
        part_resp = self.client.upload_part(
            Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id,
            PartNumber=1, Body=TEST_BODY, ChecksumCRC32=part_checksum)
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('ChecksumCRC32', part_resp)

        # Checksum request fields must not convert this legacy MPU.
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id,
            MultipartUpload={'Parts': [{
                'ETag': part_resp['ETag'], 'PartNumber': 1,
                'ChecksumCRC32': part_checksum}]},
            ChecksumCRC32=make_composite_checksum(crc32, (TEST_BODY,)),
            ChecksumType='COMPOSITE')
        self.assertEqual(200, complete_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('ChecksumCRC32', complete_resp)
        self.assertNotIn('ChecksumType', complete_resp)
        get_resp = self.client.get_object(
            Bucket=self.bucket_name, Key=obj_name, ChecksumMode='ENABLED')
        self.assertEqual(TEST_BODY, get_resp['Body'].read())
        self.assertNotIn('ChecksumCRC32', get_resp)
        self.assertNotIn('ChecksumType', get_resp)

    @skip_if_checksum_persistence_disabled
    def test_zero_length_final_part(self):
        obj_name = self.create_name('full-object-mpu-empty-final-part')
        first_part = MPU_PART_BODIES[0]
        final_part = b''
        upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')

        parts = []
        for part_number, body in enumerate((first_part, final_part), 1):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumCRC32=_crc32(body)))

        expected_checksum = make_full_object_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(first_part, final_part))
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts},
            ChecksumCRC32=expected_checksum,
            ChecksumType='FULL_OBJECT')
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='FULL_OBJECT')

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=first_part,
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='FULL_OBJECT')

    @skip_if_checksum_persistence_disabled
    def test_complete_composite_checksum_without_part_count(self):
        obj_name = self.create_name('composite-mpu-no-part-count')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        parts = []
        for part_number, body in enumerate(MPU_PART_BODIES, 1):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumCRC32=_crc32(body)))

        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=MPU_PART_BODIES)
        # The -<part_count> suffix is optional on the request header; the
        # suffixed form is still what gets reported and persisted.
        bare_checksum = expected_checksum.rpartition('-')[0]
        self.assertNotEqual(expected_checksum, bare_checksum)

        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts},
            ChecksumCRC32=bare_checksum,
            ChecksumType='COMPOSITE')
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=b''.join(MPU_PART_BODIES),
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='COMPOSITE')

    def _test_complete_checksum_bad_value(self, checksum_type):
        obj_name = self.create_name(
            '%s-mpu-bad-value' % checksum_type.lower())
        upload_id = self.create_mpu(obj_name, 'CRC32', checksum_type)

        parts = []
        for part_number, body in enumerate(MPU_PART_BODIES, 1):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumCRC32=_crc32(body)))

        if checksum_type == 'COMPOSITE':
            expected_checksum = make_composite_checksum(
                checksum_hasher_constructor=crc32,
                part_bodies=MPU_PART_BODIES)
            digest = expected_checksum.rpartition('-')[0]
        else:
            expected_checksum = make_full_object_checksum(
                checksum_hasher_constructor=crc32,
                part_bodies=MPU_PART_BODIES)
            digest = expected_checksum

        # A malformed value is rejected before checksum comparison, even when
        # its digest is wrong. The error is InvalidRequest, not BadDigest.
        for request_checksum in ('%s-0' % digest, '%s--1' % digest,
                                 '%s-abc' % digest, '%s-' % digest,
                                 '%s-1.0' % digest, '-', '-2', '!!!!',
                                 '%s-abc' % _crc32(b'wrong')):
            with self.subTest(request_checksum=request_checksum):
                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.complete_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        UploadId=upload_id,
                        MultipartUpload={'Parts': parts},
                        ChecksumCRC32=request_checksum,
                        ChecksumType=checksum_type)
                self.assert_error(
                    resp=caught.exception.response,
                    err_code='InvalidRequest',
                    err_msg='Value for x-amz-checksum-crc32 header is '
                            'invalid.',
                    obj_name=obj_name)

        # A valid retry must still complete the same upload after these errors.
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts})
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type=checksum_type)

    @skip_if_checksum_persistence_disabled
    def test_complete_full_object_checksum_bad_value(self):
        self._test_complete_checksum_bad_value('FULL_OBJECT')

    @skip_if_checksum_persistence_disabled
    def test_complete_composite_checksum_bad_value(self):
        self._test_complete_checksum_bad_value('COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_upload_file_default_checksum_persistence(self):
        obj_name = self.create_name('upload-file-mpu-default-checksum')
        body = b''.join(MPU_PART_BODIES)
        chunk_size = len(MPU_PART_BODIES[0])
        config = TransferConfig(
            multipart_threshold=chunk_size,
            multipart_chunksize=chunk_size)

        # upload_file uses a CRC32 COMPOSITE checksum by default
        with NamedTemporaryFile() as fp:
            fp.write(body)
            fp.flush()
            upload_result = self.client.upload_file(
                fp.name,
                self.bucket_name,
                obj_name,
                Config=config)
        self.assertIsNone(upload_result)

        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=MPU_PART_BODIES)

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=body,
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='COMPOSITE')

    def test_no_checksum_upload_part_checksum_not_echoed(self):
        # A legacy MPU must accept these fields with writes on or off.
        final_checksum = make_composite_checksum(crc32, (TEST_BODY,))
        for include_part_checksum, checksum_headers in (
                (False, {'ChecksumType': 'COMPOSITE'}),
                (True, {}),
                (True, {'ChecksumType': 'COMPOSITE'}),
                (False, {'ChecksumCRC32': final_checksum}),
                (True, {'ChecksumType': 'COMPOSITE',
                        'ChecksumCRC32': final_checksum})):
            with self.subTest(part_checksum=include_part_checksum,
                              headers=checksum_headers):
                obj_name = self.create_name(
                    'no-checksum-mpu-request-checksums')
                upload_id = self.create_mpu(obj_name)

                # A supplied checksum must not enroll a legacy MPU.
                part_resp = self.client.upload_part(
                    Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id,
                    PartNumber=1, Body=TEST_BODY,
                    ChecksumCRC32=_crc32(TEST_BODY))
                self.assertEqual(200, part_resp[
                    'ResponseMetadata']['HTTPStatusCode'])
                self.assertNotIn('ChecksumCRC32', part_resp)
                self.assertNotIn('ChecksumType', part_resp)
                headers = part_resp['ResponseMetadata']['HTTPHeaders']
                self.assertNotIn('x-amz-checksum-crc32', headers)
                self.assertNotIn('x-amz-checksum-type', headers)
                part = {'ETag': part_resp['ETag'], 'PartNumber': 1}
                if include_part_checksum:
                    part['ChecksumCRC32'] = _crc32(TEST_BODY)

                # Completion stores the data without a checksum response.
                complete_resp = self.client.complete_multipart_upload(
                    Bucket=self.bucket_name, Key=obj_name, UploadId=upload_id,
                    MultipartUpload={'Parts': [part]}, **checksum_headers)
                self.assertEqual(200, complete_resp[
                    'ResponseMetadata']['HTTPStatusCode'])
                self.assertNotIn('ChecksumCRC32', complete_resp)
                self.assertNotIn('ChecksumType', complete_resp)
                get_resp = self.client.get_object(
                    Bucket=self.bucket_name, Key=obj_name,
                    ChecksumMode='ENABLED')
                self.assertEqual(TEST_BODY, get_resp['Body'].read())
                self.assertNotIn('ChecksumCRC32', get_resp)
                self.assertNotIn('ChecksumType', get_resp)

    @skip_if_checksum_persistence_disabled
    def test_complete_retry_ignores_any_request_checksums_and_types(self):
        obj_name = self.create_name('mpu-complete-retry-ignores-checksums')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        parts = []
        for part_number, body in enumerate(MPU_PART_BODIES, 1):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumCRC32=_crc32(body)))

        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=MPU_PART_BODIES)
        bad_checksum = '%s-%d' % (
            _crc32(b'wrong'), len(MPU_PART_BODIES))
        self.assertNotEqual(expected_checksum, bad_checksum)
        sha256_parts = [
            {
                'ETag': part['ETag'],
                'PartNumber': part['PartNumber'],
                'ChecksumSHA256': base64_str(
                    hashlib.sha256(body).digest()),
            }
            for part, body in zip(parts, MPU_PART_BODIES)
        ]
        sha256_checksum = base64_str(
            hashlib.sha256(b''.join(MPU_PART_BODIES)).digest())

        # While the upload is still in progress, a final checksum that is
        # not the one that was calculated is rejected
        with self.assertRaises(
                botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
                ChecksumCRC32=bad_checksum,
                ChecksumType='COMPOSITE')
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest',
            err_msg='The crc32 you specified did not match the '
                    'calculated checksum.',
            obj_name=obj_name)

        # As are parts echoed with an algorithm the upload was not
        # created with
        with self.assertRaises(
                botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': sha256_parts})
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest',
            err_msg='The sha256 you specified for part 1 did not match '
                    'what we received.',
            obj_name=obj_name)

        # As is a checksum type the upload was not created with
        with self.assertRaises(
                botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts},
                ChecksumType='FULL_OBJECT')
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg='The upload was created using the COMPOSITE checksum '
                    'mode. The complete request must use the same '
                    'checksum mode.',
            obj_name=obj_name)

        # None of those rejections invalidated the upload
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts})
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

        # A completed MPU ignores well-formed checksum values and types.
        for scenario, request_parts, kwargs in (
                ('unchanged request', parts, {}),
                ('wrong final value', parts, {
                    'ChecksumCRC32': bad_checksum,
                    'ChecksumType': 'COMPOSITE'}),
                ('wrong part algorithm', sha256_parts, {}),
                ('wrong final algorithm', parts, {
                    'ChecksumSHA256': sha256_checksum}),
                ('other valid type', parts, {'ChecksumType': 'FULL_OBJECT'})):
            with self.subTest(scenario=scenario):
                retry_resp = self.client.complete_multipart_upload(
                    Bucket=self.bucket_name, Key=obj_name,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': request_parts}, **kwargs)
                self.assertEqual(
                    200, retry_resp['ResponseMetadata']['HTTPStatusCode'])
                self.assertEqual(self.bucket_name, retry_resp['Bucket'])
                self.assertEqual(obj_name, retry_resp['Key'])
                self.assertEqual(complete_resp['ETag'], retry_resp['ETag'])
                self.assertNotIn('ChecksumCRC32', retry_resp)
                self.assertNotIn('ChecksumSHA256', retry_resp)
                self.assertNotIn('ChecksumType', retry_resp)

        # But a header S3 cannot parse is still rejected on retry, even
        # though there is nothing left to compare it against
        for kwargs, err_msg in (
                ({'ChecksumType': 'bogus'},
                 'Value for x-amz-checksum-type header is invalid.'),
                ({'ChecksumCRC32': '!!!!'},
                 'Value for x-amz-checksum-crc32 header is invalid.'),
                ({'ChecksumCRC32': '%s-abc' % _crc32(b'wrong')},
                 'Value for x-amz-checksum-crc32 header is invalid.')):
            with self.subTest(**kwargs):
                with self.assertRaises(
                        botocore.exceptions.ClientError) as caught:
                    self.client.complete_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        UploadId=upload_id,
                        MultipartUpload={'Parts': parts},
                        **kwargs)
                self.assert_error(
                    resp=caught.exception.response,
                    err_code='InvalidRequest',
                    err_msg=err_msg)

        # None of the ignored checksums replaced the stored one
        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=b''.join(MPU_PART_BODIES),
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='COMPOSITE')

    def test_complete_part_multi_checksum(self):
        obj_name = self.create_name('multi-checksum-complete-mpu')
        upload_id = self.create_mpu(obj_name)
        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY)

        with self.assertRaises(
                botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                MultipartUpload={
                    'Parts': [{
                        'ETag': part['ETag'],
                        'PartNumber': part['PartNumber'],
                        'ChecksumCRC32': TestObjectChecksumCRC32.EXPECTED,
                        'ChecksumCRC32C':
                            TestObjectChecksumCRC32C.EXPECTED,
                    }],
                },
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidArgument',
            err_msg=(
                'Invalid Base64 or multiple checksums present in request'),
            obj_name=obj_name,
            ArgumentName='Checksum',
            ArgumentValue='CRC32:y/Q5Jg==;CRC32C:4waSgw==;')

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
    def test_composite_nonconsecutive_parts_error(self):
        obj_name = self.create_name('crc32-mpu-composite-nonconsecutive')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        parts = []
        # Uploading parts 1 and 3, not 2, should fail once completed.
        for part_number, body in zip([1, 3], MPU_PART_BODIES):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumAlgorithm='CRC32'))

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InternalError',
            err_msg='We encountered an internal error. Please try again.',
            status_code=500)

    @skip_if_checksum_persistence_disabled
    def test_composite_nonconsecutive_parts_defer_to_client_errors(self):
        # A non-consecutive COMPOSITE part list is a 500, but S3 only reaches
        # that check once every other part validation has passed. A genuine
        # client error in the same request is still reported as a 4xx.
        obj_name = self.create_name(
            'crc32-mpu-composite-nonconsecutive-precedence')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        parts = []
        # Uploading parts 1 and 3, not 2, so the completed list is
        # non-consecutive.
        for part_number, body in zip([1, 3], MPU_PART_BODIES):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumAlgorithm='CRC32'))

        # A part checksum that is not valid base64
        malformed = [dict(part) for part in parts]
        malformed[0]['ChecksumCRC32'] = 'not-base64!'
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': malformed},
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidArgument')

        # A part checksum that does not match the uploaded part. S3 only
        # reports this ahead of the 500 when the mismatch is on the final
        # part of the list, so that is what is asserted here; Swift
        # reports it wherever it occurs.
        mismatched = [dict(part) for part in parts]
        mismatched[-1]['ChecksumCRC32'] = _crc32(b'not the part body')
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': mismatched},
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidPart')

        # An omitted part checksum
        omitted = [dict(part) for part in parts]
        del omitted[-1]['ChecksumCRC32']
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': omitted},
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg=(
                'The upload was created using a %s checksum. The complete '
                'request must include the checksum for each part. It was '
                'missing for part %d in the request.' % (
                    'crc32', parts[-1]['PartNumber'])))

        # A final checksum value that does not match
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id,
                ChecksumCRC32=base64_str(b'\x00' * 4) + '-2')
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest')

        # With no client error left to report, the non-consecutive part
        # list is finally a 500.
        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=MPU_PART_BODIES)
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id,
                ChecksumCRC32=expected_checksum)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InternalError',
            err_msg='We encountered an internal error. Please try again.',
            status_code=500,
            obj_name=obj_name)

    @skip_if_checksum_persistence_disabled
    def test_composite_checksum_out_of_order_parts_error(self):
        obj_name = self.create_name('crc32-mpu-composite-out-of-order')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        parts = []
        for part_number, body in enumerate(MPU_PART_BODIES, 1):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumAlgorithm='CRC32'))

        # A descending list is an ordering error even though it is also
        # non-consecutive: S3 reports the ordering error it reaches first
        # rather than the 500 that a non-consecutive composite list earns.
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': list(reversed(parts))},
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidPartOrder',
            obj_name=obj_name)

        # As is a list that repeats a part number
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': [parts[0], parts[0]]},
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidPartOrder',
            obj_name=obj_name)

        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={'Parts': parts},
            UploadId=upload_id)
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=make_composite_checksum(
                checksum_hasher_constructor=crc32,
                part_bodies=MPU_PART_BODIES),
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_complete_subset_of_uploaded_parts(self):
        obj_name = self.create_name('mpu-complete-subset-of-parts')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part_bodies = (
            b'a' * MIN_MPU_PART_SIZE,
            b'included final part',
            b'omitted part',
        )
        parts = []
        for part_number, body in enumerate(part_bodies, 1):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumCRC32=_crc32(body)))

        # Part 3 was uploaded, but excluding it from the completion
        # request must exclude it from the object and final checksum.
        completed_parts = parts[:2]
        completed_part_bodies = part_bodies[:2]
        expected_body = b''.join(completed_part_bodies)
        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=completed_part_bodies)
        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            MultipartUpload={'Parts': completed_parts},
            UploadId=upload_id,
            ChecksumCRC32=expected_checksum,
            ChecksumType='COMPOSITE')
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_mpu_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=expected_body,
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_reuploaded_part_replaces_checksum(self):
        obj_name = self.create_name('mpu-reuploaded-part')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        # Upload part 1, then replace it before completion.
        original_body = MPU_PART_BODIES[0]
        original_checksum = _crc32(original_body)
        original_part = self.upload_part(
            obj_name, upload_id, 1, original_body,
            ChecksumCRC32=original_checksum)

        replacement_body = b'b' * len(original_body)
        replacement_checksum = _crc32(replacement_body)
        replacement_part = self.upload_part(
            obj_name, upload_id, 1, replacement_body,
            ChecksumCRC32=replacement_checksum)
        self.assertNotEqual(original_part['ETag'],
                            replacement_part['ETag'])
        self.assertNotEqual(original_checksum, replacement_checksum)

        # Complete with the replacement and the final part only.
        last_body = MPU_PART_BODIES[1]
        last_checksum = _crc32(last_body)
        last_part = self.upload_part(
            obj_name, upload_id, 2, last_body,
            ChecksumCRC32=last_checksum)

        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(replacement_body, last_body))
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            MultipartUpload={'Parts': [replacement_part, last_part]},
            UploadId=upload_id)
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=replacement_body + last_body,
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_bad_final_checksum_can_retry(self):
        obj_name = self.create_name('mpu-bad-final-checksum-retry')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)
        multipart_upload = {'Parts': [part]}

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                MultipartUpload=multipart_upload,
                UploadId=upload_id,
                ChecksumCRC32=TestObjectChecksumCRC32.BAD,
                ChecksumType='FULL_OBJECT')
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest',
            err_msg=(
                'The crc32 you specified did not match '
                'the calculated checksum.'))

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.head_object(
                Bucket=self.bucket_name,
                Key=obj_name,
                ChecksumMode='ENABLED')
        self.assert_error(
            resp=caught.exception.response,
            err_code='404',
            err_msg='Not Found',
            status_code=404)

        list_parts_resp = self.client.list_parts(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id)
        self.assertEqual(200, list_parts_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(1, len(list_parts_resp['Parts']))
        self.assertEqual(1, list_parts_resp['Parts'][0]['PartNumber'])
        self.assertEqual(
            part['ETag'], list_parts_resp['Parts'][0]['ETag'])

        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            MultipartUpload=multipart_upload,
            UploadId=upload_id,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED,
            ChecksumType='FULL_OBJECT')
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=TestObjectChecksumCRC32.EXPECTED,
            checksum_field='ChecksumCRC32',
            checksum_type='FULL_OBJECT')

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=TEST_BODY,
            checksum_algorithm='CRC32',
            expected_checksum=TestObjectChecksumCRC32.EXPECTED,
            checksum_type='FULL_OBJECT')

    @skip_if_checksum_persistence_disabled
    def test_complete_invalid_part_checksum(self):
        obj_name = self.create_name('mpu-complete-invalid-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={
                    'Parts': [
                        {
                            'ETag': part['ETag'],
                            'PartNumber': part['PartNumber'],
                            'ChecksumCRC32':
                                TestObjectChecksumCRC32.INVALID,
                        },
                    ],
                },
                UploadId=upload_id)
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidArgument',
            err_msg=(
                'Invalid Base64 or multiple checksums present in '
                'request'))

    @skip_if_checksum_persistence_disabled
    def test_complete_bad_part_checksum_with_bad_request_final_checksum(
            self):
        obj_name = self.create_name(
            'mpu-bad-part-checksum-bad-final-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)

        final_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(TEST_BODY,))
        request_checksum = '%s-1' % _crc32(b'wrong')
        self.assertNotEqual(final_checksum, request_checksum)
        self.assertNotEqual(
            part['ChecksumCRC32'],
            TestObjectChecksumCRC32.BAD)
        # Even though we have a bad checksum on part,
        # a final checksum mismatch (aka BadDigest) is raised
        # before that bad part... yea this is funky
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [{
                        'ETag': part['ETag'],
                        'PartNumber': part['PartNumber'],
                        'ChecksumCRC32':
                            TestObjectChecksumCRC32.BAD,
                    }],
                },
                ChecksumCRC32=request_checksum,
                ChecksumType='COMPOSITE')
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest',
            err_msg=(
                'The crc32 you specified did not match '
                'the calculated checksum.'))

    @skip_if_checksum_persistence_disabled
    def test_complete_bad_part_checksum_can_retry(self):
        obj_name = self.create_name('mpu-bad-part-checksum-retry')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [{
                        'ETag': part['ETag'],
                        'PartNumber': part['PartNumber'],
                        'ChecksumCRC32':
                            TestObjectChecksumCRC32.BAD,
                    }],
                })
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidPart',
            err_msg=(
                'One or more of the specified parts could not be found.  '
                'The part may not have been uploaded, or the specified '
                "entity tag may not match the part's entity tag."),
            UploadId=upload_id,
            PartNumber='1',
            ETag=part['ETag'].strip('"'))

        expected_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(TEST_BODY,))
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': [part]})
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_complete_bad_request_part_checksum_wins_over_size(
            self):
        obj_name = self.create_name(
            'mpu-too-small-part-bad-request-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')
        first_body = TEST_BODY
        first_part = self.upload_part(
            obj_name, upload_id, 1, first_body,
            ChecksumCRC32=_crc32(first_body))

        final_body = b'final part'
        final_part = self.upload_part(
            obj_name, upload_id, 2, final_body,
            ChecksumCRC32=_crc32(final_body))

        self.assertLess(len(first_body), MIN_MPU_PART_SIZE)
        self.assertNotEqual(
            first_part['ChecksumCRC32'], TestObjectChecksumCRC32.BAD)
        # Just looking at one part, S3 raises checksum errors
        #  before any size errors on that part
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [
                        {
                            'ETag': first_part['ETag'],
                            'PartNumber': first_part['PartNumber'],
                            'ChecksumCRC32':
                                TestObjectChecksumCRC32.BAD,
                        },
                        final_part,
                    ],
                })
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidPart',
            err_msg=(
                'One or more of the specified parts could not be found.  '
                'The part may not have been uploaded, or the specified '
                "entity tag may not match the part's entity tag."))

    @skip_if_checksum_persistence_disabled
    def test_complete_size_wins_over_earlier_bad_request_part_checksum(
            self):
        obj_name = self.create_name(
            'mpu-bad-request-checksum-before-too-small-part')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        first_body = MPU_PART_BODIES[0]
        first_part = self.upload_part(
            obj_name, upload_id, 1, first_body,
            ChecksumCRC32=_crc32(first_body))

        second_body = TEST_BODY
        second_part = self.upload_part(
            obj_name, upload_id, 2, second_body,
            ChecksumCRC32=_crc32(second_body))

        final_body = b'final part'
        final_part = self.upload_part(
            obj_name, upload_id, 3, final_body,
            ChecksumCRC32=_crc32(final_body))

        self.assertGreaterEqual(len(first_body), MIN_MPU_PART_SIZE)
        self.assertLess(len(second_body), MIN_MPU_PART_SIZE)
        self.assertNotEqual(
            first_part['ChecksumCRC32'], TestObjectChecksumCRC32.BAD)
        # If in the request manifest we have a size error on one of the
        # specifcied parts, BUT a part is defined before that only has
        # a checksum error, then that is the one S3 raises
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [
                        {
                            'ETag': first_part['ETag'],
                            'PartNumber': first_part['PartNumber'],
                            'ChecksumCRC32':
                                TestObjectChecksumCRC32.BAD,
                        },
                        second_part,
                        final_part,
                    ],
                })
        self.assert_error(
            resp=caught.exception.response,
            err_code='EntityTooSmall')

    @skip_if_checksum_persistence_disabled
    def test_complete_size_wins_over_later_bad_request_part_checksum(
            self):
        obj_name = self.create_name(
            'mpu-too-small-part-before-bad-request-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        first_body = TEST_BODY
        first_part = self.upload_part(
            obj_name, upload_id, 1, first_body,
            ChecksumCRC32=_crc32(first_body))

        final_body = b'final part'
        final_part = self.upload_part(
            obj_name, upload_id, 2, final_body,
            ChecksumCRC32=_crc32(final_body))

        # If we later have a checksum error in some part,
        # but the current part we are looking at has a size error,
        # S3 raises that
        self.assertLess(len(first_body), MIN_MPU_PART_SIZE)
        self.assertNotEqual(
            final_part['ChecksumCRC32'], TestObjectChecksumCRC32.BAD)
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [
                        first_part,
                        {
                            'ETag': final_part['ETag'],
                            'PartNumber': final_part['PartNumber'],
                            'ChecksumCRC32':
                                TestObjectChecksumCRC32.BAD,
                        },
                    ],
                })
        self.assert_error(
            resp=caught.exception.response,
            err_code='EntityTooSmall')

    @skip_if_checksum_persistence_disabled
    def test_complete_too_small_part_with_good_request_part_checksums(
            self):
        obj_name = self.create_name(
            'mpu-too-small-part-good-request-checksums')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        first_body = TEST_BODY
        first_part = self.upload_part(
            obj_name, upload_id, 1, first_body,
            ChecksumCRC32=_crc32(first_body))

        final_body = b'final part'
        final_part = self.upload_part(
            obj_name, upload_id, 2, final_body,
            ChecksumCRC32=_crc32(final_body))

        self.assertLess(len(first_body), MIN_MPU_PART_SIZE)
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': [first_part, final_part]})
        self.assert_error(
            resp=caught.exception.response,
            err_code='EntityTooSmall')

    @skip_if_checksum_persistence_disabled
    def test_complete_too_small_part_with_good_request_final_checksum(
            self):
        obj_name = self.create_name(
            'mpu-too-small-part-good-final-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        first_body = TEST_BODY
        first_part = self.upload_part(
            obj_name, upload_id, 1, first_body,
            ChecksumCRC32=_crc32(first_body))

        final_body = b'final part'
        final_part = self.upload_part(
            obj_name, upload_id, 2, final_body,
            ChecksumCRC32=_crc32(final_body))

        final_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(first_body, final_body))
        self.assertLess(len(first_body), MIN_MPU_PART_SIZE)
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': [first_part, final_part]},
                ChecksumCRC32=final_checksum,
                ChecksumType='COMPOSITE')
        self.assert_error(
            resp=caught.exception.response,
            err_code='EntityTooSmall')

    @skip_if_checksum_persistence_disabled
    def test_complete_too_small_part_with_bad_request_final_checksum(
            self):
        obj_name = self.create_name(
            'mpu-too-small-part-bad-final-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        first_body = TEST_BODY
        first_part = self.upload_part(
            obj_name, upload_id, 1, first_body,
            ChecksumCRC32=_crc32(first_body))

        final_body = b'final part'
        final_part = self.upload_part(
            obj_name, upload_id, 2, final_body,
            ChecksumCRC32=_crc32(final_body))

        final_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(first_body, final_body))
        request_checksum = '%s-2' % _crc32(b'wrong')
        self.assertNotEqual(final_checksum, request_checksum)
        self.assertLess(len(first_body), MIN_MPU_PART_SIZE)
        # Who knows why... but S3 raises a final checksum mismatch
        # error BEFORE checking the sizes of any part
        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': [first_part, final_part]},
                ChecksumCRC32=request_checksum,
                ChecksumType='COMPOSITE')
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest',
            err_msg=(
                'The crc32 you specified did not match '
                'the calculated checksum.'))

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
        expected_checksum = make_full_object_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=MPU_PART_BODIES)
        # Each completion uses a fresh MPU with no client part checksums.
        for complete_kwargs in (
                {'ChecksumCRC32': expected_checksum,
                 'ChecksumType': 'FULL_OBJECT'}, {}):
            with self.subTest(complete_kwargs=complete_kwargs):
                obj_name = self.create_name(
                    'mpu-full-object-upload-part-checksum-optional')
                upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')
                parts = self._upload_parts_without_checksum(
                    obj_name, upload_id)
                with self.disable_client_checksum_calculation():
                    complete_resp = self.client.complete_multipart_upload(
                        Bucket=self.bucket_name,
                        Key=obj_name,
                        MultipartUpload={'Parts': parts},
                        UploadId=upload_id,
                        **complete_kwargs)
                self._assert_checksum_complete_mpu(
                    complete_mpu_resp=complete_resp,
                    expected_checksum=expected_checksum,
                    checksum_field='ChecksumCRC32',
                    checksum_type='FULL_OBJECT')

    @skip_if_checksum_persistence_disabled
    def test_composite_complete_requires_part_checksums(self):
        obj_name = self.create_name(
            'mpu-composite-complete-missing-part-checksum')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                # We need to include checksums here
                MultipartUpload={'Parts': [{
                    'ETag': part['ETag'],
                    'PartNumber': 1,
                }]})
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg=(
                'The upload was created using a crc32 checksum. The '
                'complete request must include the checksum for each '
                'part. It was missing for part 1 in the request.'),
            obj_name=obj_name)

    def _upload_parts_without_checksum(self, obj_name, upload_id):
        with self.disable_client_checksum_calculation():
            parts = []
            for part_number, body in enumerate(
                    MPU_PART_BODIES, 1):
                part_resp = self.client.upload_part(
                    Bucket=self.bucket_name,
                    Key=obj_name,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    # No part checksum; the server calculates it for us
                    Body=body)
                self._assert_checksum_upload_part_mpu(
                    part_resp=part_resp,
                    part_checksum=_crc32(body),
                    checksum_header='x-amz-checksum-crc32',
                    checksum_field='ChecksumCRC32')
                parts.append({
                    'ETag': part_resp['ETag'],
                    # No part checksum here
                    'PartNumber': part_number,
                })
        return parts

    @skip_if_checksum_persistence_disabled
    def test_full_object_upload_part_no_checksum_complete_bad_checksum(self):
        obj_name = self.create_name(
            'mpu-full-object-upload-part-checksum-optional')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')
        parts = self._upload_parts_without_checksum(obj_name, upload_id)
        bad_checksum = make_full_object_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=[])
        with self.disable_client_checksum_calculation():
            with self.assertRaises(botocore.exceptions.ClientError) as caught:
                self.client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=obj_name,
                    MultipartUpload={'Parts': parts},
                    UploadId=upload_id,
                    ChecksumCRC32=bad_checksum,
                    ChecksumType='FULL_OBJECT')
        self.assert_error(
            resp=caught.exception.response,
            err_code='BadDigest',
            err_msg=('The crc32 you specified did not match the calculated '
                     'checksum.'),
            obj_name=obj_name)

    @skip_if_checksum_persistence_disabled
    def test_full_object_part_checksums_independently_optional(self):
        obj_name = self.create_name(
            'mpu-full-object-some-part-checksums')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')
        with self.disable_client_checksum_calculation():
            part_one_body, part_two_body = MPU_PART_BODIES
            part_one = self.upload_part(
                obj_name, upload_id, 1, part_one_body,
                ChecksumCRC32=_crc32(part_one_body))

            part_two_resp = self.client.upload_part(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                PartNumber=2,
                Body=part_two_body)
            self._assert_checksum_upload_part_mpu(
                part_resp=part_two_resp,
                part_checksum=_crc32(part_two_body),
                checksum_header='x-amz-checksum-crc32',
                checksum_field='ChecksumCRC32')

            expected_body = part_one_body + part_two_body
            expected_checksum = make_full_object_checksum(
                checksum_hasher_constructor=crc32,
                part_bodies=(part_one_body, part_two_body))
            # Since full object checksums can optionally supply the
            # checksums in the request manifest, we can supply checksums
            # for SOME parts
            complete_resp = self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=obj_name,
                UploadId=upload_id,
                MultipartUpload={'Parts': [
                    part_one,
                    {
                        'ETag': part_two_resp['ETag'],
                        'PartNumber': 2,
                    },
                ]})
            self._assert_checksum_complete_mpu(
                complete_mpu_resp=complete_resp,
                expected_checksum=expected_checksum,
                checksum_field='ChecksumCRC32',
                checksum_type='FULL_OBJECT')
            self._assert_mpu_checksum_persistence(
                obj_name=obj_name,
                expected_body=expected_body,
                checksum_algorithm='CRC32',
                expected_checksum=expected_checksum,
                checksum_type='FULL_OBJECT')

    @skip_if_checksum_persistence_disabled
    def test_full_object_allows_nonconsecutive_parts(self):
        obj_name = self.create_name(
            'mpu-full-object-nonconsecutive-parts')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'FULL_OBJECT')

        parts = []
        for part_number, body in zip(
                (1, 3), MPU_PART_BODIES):
            parts.append(self.upload_part(
                obj_name, upload_id, part_number, body,
                ChecksumCRC32=_crc32(body)))

        expected_body = b''.join(MPU_PART_BODIES)
        expected_checksum = make_full_object_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=MPU_PART_BODIES)
        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            MultipartUpload={'Parts': parts},
            UploadId=upload_id,
            ChecksumCRC32=expected_checksum,
            ChecksumType='FULL_OBJECT')
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_resp,
            expected_checksum=expected_checksum,
            checksum_field='ChecksumCRC32',
            checksum_type='FULL_OBJECT')

        self._assert_mpu_checksum_persistence(
            obj_name=obj_name,
            expected_body=expected_body,
            checksum_algorithm='CRC32',
            expected_checksum=expected_checksum,
            checksum_type='FULL_OBJECT')

    def test_complete_rejects_unknown_checksum_algorithm(self):
        obj_name = self.create_name('mpu-complete-unknown-checksum-algorithm')
        upload_id = self.create_mpu(obj_name)

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY)
        part['ChecksumCRC32'] = _crc32(TEST_BODY)

        # Bypass SDK validation to test an unknown XML checksum element.
        def inject_unknown_checksum(params, **_kwargs):
            """Replace the modeled checksum XML element before signing."""
            body = params['body']
            # Swapping the known element to the unknown element
            # should give us an error
            known_element = b'ChecksumCRC32'
            unknown_element = b'ChecksumCRC31'
            if isinstance(body, str):
                known_element = known_element.decode('ascii')
                unknown_element = unknown_element.decode('ascii')
            self.assertIn(known_element, body)
            params['body'] = body.replace(known_element, unknown_element)

        event_name = 'before-call.s3.CompleteMultipartUpload'
        self.client.meta.events.register(event_name, inject_unknown_checksum)
        try:
            with self.assertRaises(botocore.exceptions.ClientError) as caught:
                self.client.complete_multipart_upload(
                    Bucket=self.bucket_name, Key=obj_name,
                    MultipartUpload={'Parts': [part]},
                    UploadId=upload_id)
            self.assert_error(
                resp=caught.exception.response,
                err_code='MalformedXML',
                err_msg=(
                    'The XML you provided was not well-formed or did not '
                    'validate against our published schema'))
        finally:
            self.client.meta.events.unregister(
                event_name, inject_unknown_checksum)

    def test_complete_rejects_unknown_checksum_type(self):
        # The type is checked for membership before it is compared against the
        # upload, so this is not the checksum type mismatch error.
        obj_name = self.create_name('mpu-complete-unknown-checksum-type')
        upload_id = self.create_mpu(obj_name)

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY)

        with self.assertRaises(botocore.exceptions.ClientError) as caught:
            self.client.complete_multipart_upload(
                Bucket=self.bucket_name, Key=obj_name,
                MultipartUpload={'Parts': [part]},
                UploadId=upload_id,
                # Not a valid checksum type
                ChecksumType='cheese fries')
        self.assert_error(
            resp=caught.exception.response,
            err_code='InvalidRequest',
            err_msg='Value for x-amz-checksum-type header is invalid.')

    @skip_if_checksum_persistence_disabled
    def test_complete_normalizes_composite_checksum_type(self):
        obj_name = self.create_name('mpu-complete-normalize-checksum-type')
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part = self.upload_part(
            obj_name, upload_id, 1, TEST_BODY,
            ChecksumCRC32=TestObjectChecksumCRC32.EXPECTED)

        complete_mpu_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name, Key=obj_name,
            MultipartUpload={'Parts': [part]},
            UploadId=upload_id,
            # S3 normalizes this
            ChecksumType='ComPosIte')
        self._assert_checksum_complete_mpu(
            complete_mpu_resp=complete_mpu_resp,
            expected_checksum=make_composite_checksum(
                checksum_hasher_constructor=crc32, part_bodies=(TEST_BODY,)),
            checksum_field='ChecksumCRC32',
            checksum_type='COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_same_key_checksum_are_isolated(self):
        obj_name = self.create_name('same-key-checksum-mpus')
        first_upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')
        second_upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')
        self.assertNotEqual(first_upload_id, second_upload_id)

        # Both uploads are in progress before either one completes.
        uploads = []
        for upload_id, body in (
                (first_upload_id, TEST_BODY),
                (second_upload_id, b'a different multipart object')):
            part = self.upload_part(
                obj_name, upload_id, 1, body, ChecksumCRC32=_crc32(body))
            uploads.append((upload_id, body, part))

        # Each completed upload must use its own part checksum.
        for upload_id, body, part in uploads:
            with self.subTest(upload_id=upload_id):
                expected_checksum = make_composite_checksum(crc32, (body,))
                complete_resp = self.client.complete_multipart_upload(
                    Bucket=self.bucket_name, Key=obj_name,
                    UploadId=upload_id, MultipartUpload={'Parts': [part]})
                self.assertEqual(
                    200, complete_resp['ResponseMetadata']['HTTPStatusCode'])
                self.assertEqual(
                    expected_checksum, complete_resp['ChecksumCRC32'])
                self._assert_mpu_checksum_persistence(
                    obj_name=obj_name, expected_body=body,
                    checksum_algorithm='CRC32',
                    expected_checksum=expected_checksum,
                    checksum_type='COMPOSITE')

    @skip_if_checksum_persistence_disabled
    def test_checksum_overwrite_replaces_old_checksum_metadata(self):
        obj_name = self.create_name('mpu-overwrite-checksum-metadata')
        original_body = b'original sha256 object'
        original_checksum = base64_str(
            hashlib.sha256(original_body).digest())
        put_resp = self.client.put_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            Body=original_body,
            ChecksumSHA256=original_checksum)
        self.assertEqual(200, put_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(original_checksum, put_resp['ChecksumSHA256'])

        replacement_body = b'replacement crc32 multipart object'
        replacement_part_checksum = _crc32(replacement_body)
        replacement_checksum = make_composite_checksum(
            checksum_hasher_constructor=crc32,
            part_bodies=(replacement_body,))
        upload_id = self.create_mpu(obj_name, 'CRC32', 'COMPOSITE')

        part_resp = self.client.upload_part(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            PartNumber=1,
            Body=replacement_body,
            ChecksumCRC32=replacement_part_checksum)
        self.assertEqual(200, part_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            replacement_part_checksum, part_resp['ChecksumCRC32'])

        complete_resp = self.client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=obj_name,
            UploadId=upload_id,
            MultipartUpload={
                'Parts': [{
                    'ETag': part_resp['ETag'],
                    'PartNumber': 1,
                    'ChecksumCRC32': replacement_part_checksum,
                }],
            })
        self.assertEqual(200, complete_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            replacement_checksum, complete_resp['ChecksumCRC32'])
        self.assertNotIn('ChecksumSHA256', complete_resp)

        head_resp = self.client.head_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            ChecksumMode='ENABLED')
        self.assertEqual(200, head_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            replacement_checksum, head_resp['ChecksumCRC32'])
        self.assertEqual('COMPOSITE', head_resp['ChecksumType'])
        self.assertNotIn('ChecksumSHA256', head_resp)
        head_headers = head_resp['ResponseMetadata']['HTTPHeaders']
        self.assertNotIn('x-amz-checksum-sha256', head_headers)

        get_resp = self.client.get_object(
            Bucket=self.bucket_name,
            Key=obj_name,
            ChecksumMode='ENABLED')
        self.assertEqual(200, get_resp[
            'ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(replacement_body, get_resp['Body'].read())
        self.assertEqual(
            replacement_checksum, get_resp['ChecksumCRC32'])
        self.assertEqual('COMPOSITE', get_resp['ChecksumType'])
        self.assertNotIn('ChecksumSHA256', get_resp)
        get_headers = get_resp['ResponseMetadata']['HTTPHeaders']
        self.assertNotIn('x-amz-checksum-sha256', get_headers)
