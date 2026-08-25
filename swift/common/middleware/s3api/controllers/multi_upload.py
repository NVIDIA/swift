# Copyright (c) 2010-2014 OpenStack Foundation.
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
"""
Implementation of S3 Multipart Upload.

This module implements S3 Multipart Upload APIs with the Swift SLO feature.
The following explains how S3api uses swift container and objects to store S3
upload information:

-----------------
[bucket]+segments
-----------------

A container to store upload information. [bucket] is the original bucket
where multipart upload is initiated.

-----------------------------
[bucket]+segments/[upload_id]
-----------------------------

An object of the ongoing upload id. The object is empty and used for
checking the target upload status. If the object exists, it means that the
upload is initiated but not either completed or aborted.

-------------------------------------------
[bucket]+segments/[upload_id]/[part_number]
-------------------------------------------

The last suffix is the part number under the upload id. When the client uploads
the parts, they will be stored in the namespace with
[bucket]+segments/[upload_id]/[part_number].

Example listing result in the [bucket]+segments container::

  [bucket]+segments/[upload_id1]  # upload id object for upload_id1
  [bucket]+segments/[upload_id1]/1  # part object for upload_id1
  [bucket]+segments/[upload_id1]/2  # part object for upload_id1
  [bucket]+segments/[upload_id1]/3  # part object for upload_id1
  [bucket]+segments/[upload_id2]  # upload id object for upload_id2
  [bucket]+segments/[upload_id2]/1  # part object for upload_id2
  [bucket]+segments/[upload_id2]/2  # part object for upload_id2
     .
     .

Those part objects are directly used as segments of a Swift
Static Large Object when the multipart upload is completed.

"""

import binascii
import copy
import os
import re
import time
from itertools import repeat

from swift.common import constraints
from swift.common.swob import Range, bytes_to_wsgi, normalize_etag, \
    wsgi_quote, wsgi_to_str, parse_date_header
from swift.common.utils import json, public, reiterate, md5, base64_str, \
    serialize_header
from swift.common.utils.timestamp import Timestamp, NormalTimestamp
from swift.common.request_helpers import get_container_update_override_key, \
    get_param

from urllib.parse import quote, urlparse

from swift.common.middleware.s3api.acl_handlers import PartAclHandler, \
    UploadAclHandler, UploadsAclHandler
from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation, object_operation, check_container_existence
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    ErrorResponse, MalformedXML, KeyTooLongError, InvalidPart, \
    BucketAlreadyExists, EntityTooSmall, InvalidPartOrder, InvalidRequest, \
    HTTPOk, HTTPNoContent, NoSuchKey, NoSuchUpload, NoSuchBucket, \
    BucketAlreadyOwnedByYou, ServiceUnavailable, PreconditionFailed, \
    S3NotImplemented, BadDigest, InternalError
from swift.common.middleware.s3api.s3checksum import \
    CHECKSUM_TYPE_FULL_OBJECT, CHECKSUM_TYPE_COMPOSITE, \
    CHECKSUMS_BY_HEADER, CHECKSUM_XML_ELEMENTS, \
    FULL_OBJECT_MPU_CHECKSUM_HEADERS, \
    COMPOSITE_MPU_CHECKSUM_HEADERS, checksum_header_from_algorithm, \
    checksum_algorithm_from_header, checksum_xml_element_from_header, \
    checksum_sysmeta_headers, format_supported_checksum_algorithms, \
    checksum_sysmeta_to_response_headers, checksum_sysmeta_values, \
    normalize_checksum_type, split_composite_checksum, \
    validate_checksum_value, get_checksum_hasher
from swift.common.middleware.s3api.utils import unique_id, \
    MULTIUPLOAD_SUFFIX, S3Timestamp, s3api_sysmeta_header
from swift.common.middleware.s3api.etree import Element, SubElement, \
    fromstring, tostring, XMLSyntaxError, DocumentInvalid
from swift.common.storage_policy import POLICIES

DEFAULT_MAX_PARTS_LISTING = 1000
DEFAULT_MAX_UPLOADS = 1000

MAX_COMPLETE_UPLOAD_BODY_SIZE = 2048 * 1024


def _validate_checksum_type_with_checksum_header(
        checksum_type, checksum_header):
    """
    Validate that an MPU checksum type supports the checksum header.

    :param checksum_type: The normalized MPU checksum type.
    :param checksum_header: The corresponding x-amz-checksum-* header.
    :raises InvalidRequest: if the algorithm does not support the type.
    """
    if ((checksum_type == CHECKSUM_TYPE_FULL_OBJECT and
         checksum_header not in FULL_OBJECT_MPU_CHECKSUM_HEADERS) or
            (checksum_type == CHECKSUM_TYPE_COMPOSITE and
             checksum_header not in COMPOSITE_MPU_CHECKSUM_HEADERS)):
        checksum_algorithm = checksum_algorithm_from_header(checksum_header)
        raise InvalidRequest(
            'The %s checksum type cannot be used with the %s '
            'checksum algorithm.' % (
                checksum_type, checksum_algorithm.lower()))


class MPUChecksumAttrs:
    """Store the checksum configuration for a multipart upload."""

    def __init__(self, checksum_header=None, checksum_value=None,
                 checksum_type=None, is_mpu_in_progress=True):
        """Initialize a multipart upload checksum configuration."""
        if checksum_header is not None:
            checksum_header = checksum_header.lower()
        self.header = checksum_header
        self.value = checksum_value
        # S3 normalizes the types
        if checksum_type is not None:
            checksum_type = normalize_checksum_type(checksum_type)
        self.type = checksum_type
        self.is_mpu_in_progress = is_mpu_in_progress

    @property
    def algorithm(self):
        """Return the normalized checksum algorithm, if configured."""
        if self.header is None:
            return None
        return checksum_algorithm_from_header(self.header)

    @property
    def is_checksum_aware(self):
        """Return whether the multipart upload has a valid checksum mode."""
        return self.is_full_object or self.is_composite

    @property
    def is_full_object(self):
        """Return whether the multipart upload uses full-object checksums."""
        return (
            self.is_mpu_in_progress and
            self.type == CHECKSUM_TYPE_FULL_OBJECT and
            self.header in FULL_OBJECT_MPU_CHECKSUM_HEADERS)

    @property
    def is_composite(self):
        """Return whether the multipart upload uses composite checksums."""
        return (
            self.is_mpu_in_progress and
            self.type == CHECKSUM_TYPE_COMPOSITE and
            self.header in COMPOSITE_MPU_CHECKSUM_HEADERS)

    @classmethod
    def from_request(cls, req):
        """
        Return an MPU checksum configuration from an S3 request.

        :param req: The S3 request.
        """
        checksum_header = checksum_value = None
        if 'uploads' in req.params:
            # CreateMultipartUpload uses x-amz-checksum-algorithm to configure
            # the checksum used by the upload.
            checksum_algorithm = req.headers.get(
                'x-amz-checksum-algorithm')
            if checksum_algorithm:
                checksum_header = checksum_header_from_algorithm(
                    checksum_algorithm)
        else:
            # UploadPart and CompleteMultipartUpload use an
            # x-amz-checksum-* value header.
            # S3Request has already validated that at most one supported value
            # header exists and that it matches x-amz-sdk-checksum-algorithm,
            # if supplied.
            checksum_header, checksum_value = \
                cls._get_checksum_value_header(req)
            if checksum_header is None and 'partNumber' in req.params:
                # Only use the trailer if this is a UploadPart request,
                # CompleteMultiPartUpload does not use the trailer
                checksum_header = cls._get_checksum_trailer_header(req)
        checksum_type = req.headers.get('x-amz-checksum-type')
        return cls(checksum_header, checksum_value, checksum_type)

    @staticmethod
    def _get_checksum_value_header(req):
        """Return the checksum value header and value from a request."""
        checksum_headers = [
            (header, value)
            for header, value in req.headers.items()
            if header.lower() in CHECKSUMS_BY_HEADER
        ]
        if checksum_headers:
            return checksum_headers[0]
        return None, None

    @staticmethod
    def _get_checksum_trailer_header(req):
        """Return the checksum header declared in x-amz-trailer."""
        checksum_trailer = req.headers.get('x-amz-trailer', '').strip()
        checksum_trailer_headers = [
            header.strip().lower()
            for header in checksum_trailer.rstrip(',').split(',')
            if header.strip().lower() in CHECKSUMS_BY_HEADER
        ] if checksum_trailer else []
        if checksum_trailer_headers:
            return checksum_trailer_headers[0]
        return None

    @classmethod
    def from_sysmeta(cls, sysmeta_headers, is_mpu_in_progress):
        """Return an MPU checksum configuration stored in object sysmeta."""
        checksum_algorithm, checksum_value, checksum_type = \
            checksum_sysmeta_values(
                sysmeta_headers)
        checksum_header = None
        if checksum_algorithm is not None:
            checksum_header = checksum_header_from_algorithm(
                checksum_algorithm)
        return cls(
            checksum_header, checksum_value, checksum_type,
            is_mpu_in_progress)


def _validate_mpu_initiate_checksum_headers(checksum_attrs):
    """
    Validate and normalize checksum headers for CreateMultipartUpload.

    :param checksum_attrs: The MPU checksum configuration to validate.
    """
    # Validate x-amz-checksum-type first...
    if checksum_attrs.type is not None and checksum_attrs.type not in (
            CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT):
        raise InvalidRequest(
            'Value for x-amz-checksum-type header is invalid.')
    # then make sure we have an x-amz-checksum-algorithm for it too
    if checksum_attrs.type and not checksum_attrs.header:
        raise InvalidRequest(
            'The x-amz-checksum-type header can only be used with the '
            'x-amz-checksum-algorithm header.')
    if not checksum_attrs.header:
        return

    if checksum_attrs.header not in CHECKSUMS_BY_HEADER:
        raise InvalidRequest(
            'Checksum algorithm provided is unsupported. Please '
            'try again with any of the valid types: %s' %
            format_supported_checksum_algorithms())

    # Decide the default checksum type for this MPU
    # which will be COMPOSITE, unless the algorithm cannot
    # be COMPOSITE
    if checksum_attrs.type is None:
        if checksum_attrs.header in COMPOSITE_MPU_CHECKSUM_HEADERS:
            checksum_attrs.type = CHECKSUM_TYPE_COMPOSITE
        else:
            checksum_attrs.type = CHECKSUM_TYPE_FULL_OBJECT

    _validate_checksum_type_with_checksum_header(
        checksum_attrs.type, checksum_attrs.header)


def _validate_mpu_complete_checksum_headers(
        request_checksum_attrs, mpu_checksum_attrs):
    """
    Validate CompleteMultipartUpload checksums against the MPU configuration.

    :param request_checksum_attrs: The request checksum configuration.
    :param mpu_checksum_attrs: The stored MPU checksum configuration.
    """
    # Validate an explicit type, including on a completed upload.
    if (request_checksum_attrs.type is not None and
            request_checksum_attrs.type not in (
                CHECKSUM_TYPE_COMPOSITE, CHECKSUM_TYPE_FULL_OBJECT)):
        raise InvalidRequest(
            'Value for x-amz-checksum-type header is invalid.')

    if mpu_checksum_attrs.is_mpu_in_progress:
        # The completion type must match the stored initiation type.
        if (request_checksum_attrs.type and
                mpu_checksum_attrs.is_checksum_aware and
                request_checksum_attrs.type != mpu_checksum_attrs.type):
            raise InvalidRequest(
                'The upload was created using the %s checksum mode. '
                'The complete request must use the same checksum mode.' %
                (mpu_checksum_attrs.type or 'null'))

        # A supplied final checksum must use the MPU's checksum algorithm.
        if (request_checksum_attrs.header and
                mpu_checksum_attrs.is_checksum_aware and
                request_checksum_attrs.header != mpu_checksum_attrs.header):
            # S3 returns the checksums in lowercase for this error
            raise InvalidRequest(
                'Checksum Type mismatch occurred, expected checksum '
                'Type: %s, actual checksum Type: %s' % (
                    mpu_checksum_attrs.algorithm.lower(),
                    request_checksum_attrs.algorithm.lower()))

    # A malformed value is rejected before it is ever compared. S3 checks this
    # even once the upload has finished, when nothing is left to compare.
    if request_checksum_attrs.header:
        _validate_complete_checksum_value(
            get_checksum_hasher(request_checksum_attrs.header),
            request_checksum_attrs.header,
            request_checksum_attrs.value)


def _validate_complete_checksum_value(checksum_hasher, checksum_header,
                                      checksum_value):
    """
    Validate the final checksum supplied to CompleteMultipartUpload.

    S3 accepts the -<part_count> suffix whatever the checksum type is.

    :param checksum_hasher: The hashing algorithm for this checksum.
    :param checksum_header: The checksum header that carried the value.
    :param checksum_value: A final checksum value.
    :raises InvalidRequest: if the digest or the -<part_count> suffix is
        malformed.
    """
    try:
        b64digest, _ = split_composite_checksum(checksum_value)
        validate_checksum_value(checksum_hasher, b64digest)
    except ValueError:
        raise InvalidRequest(
            'Value for %s header is invalid.' % checksum_header)


def _validate_part_checksum(checksum_attrs, checksum_value):
    """
    Validate and decode a part checksum.

    :param checksum_attrs: The checksum configuration that selects the
        algorithm.
    :param checksum_value: A base64 checksum value.
    :return: The decoded raw checksum bytes.
    :raises InvalidArgument: if the value is not valid for the algorithm.
    """
    checksum_hasher = get_checksum_hasher(checksum_attrs.header)
    try:
        return validate_checksum_value(checksum_hasher, checksum_value)
    except ValueError:
        # Use the S3 error message for an invalid part checksum.
        raise InvalidArgument(
            checksum_attrs.header, checksum_value,
            'Invalid Base64 or multiple checksums present in request')


def _validate_part_checksum_occurrences(part_elem):
    """
    Validate that we only have 1 checksum within the client
    MPU manifest and return that 1 checksum.

    :param part_elem: CompleteMultipartUpload ``Part`` XML element.
    :return: A ``(checksum_header, checksum_value)`` pair, or ``(None, None)``.
    :raises InvalidArgument: if the part contains multiple checksum elements.
    """
    checksum_values = []
    for checksum_header, checksum_elem_name in \
            CHECKSUM_XML_ELEMENTS.items():
        checksum_elem = part_elem.find('./%s' % checksum_elem_name)
        if checksum_elem is not None and checksum_elem.text is not None:
            checksum_values.append((checksum_header, checksum_elem.text))
    # Each part can have only one checksum element.
    if len(checksum_values) > 1:
        argument_value = ''.join(
            '%s:%s;' % (
                checksum_algorithm_from_header(checksum_header),
                checksum_value)
            for checksum_header, checksum_value in checksum_values)
        raise InvalidArgument(
            'Checksum', argument_value,
            'Invalid Base64 or multiple checksums present in request')
    if checksum_values:
        return checksum_values[0]
    return None, None


def _validate_request_manifest_checksum_part(
        part_elem, checksum_attrs, part_number):
    """
    Validate a checksum supplied for a CompleteMultipartUpload part element.

    :param part_elem: CompleteMultipartUpload ``Part`` XML element.
    :param checksum_attrs: The checksum configuration stored on the MPU.
    :param part_number: The part number associated with the checksum.
    :return: The encoded checksum value, or ``None`` when the part does not
        supply a checksum, the MPU is legacy, or it has already finished.
    """
    if not checksum_attrs.is_mpu_in_progress:
        # Once the upload has finished S3 replays it without looking at the
        # checksums the client echoed back to us.
        return None
    part_checksum_header, part_checksum_value = \
        _validate_part_checksum_occurrences(part_elem)
    if not checksum_attrs.is_checksum_aware:
        # Checksum fields do not change a legacy MPU's checksum settings.
        return None
    if (part_checksum_header is not None and
            part_checksum_header != checksum_attrs.header):
        # S3 reports an algorithm mismatch as BadDigest.
        raise BadDigest(
            'The %s you specified for part %d did not match what we '
            'received.' % (
                checksum_algorithm_from_header(part_checksum_header).lower(),
                part_number))
    if part_checksum_header is None:
        return None
    # Validate the checksum before SLO reads the parts.
    _validate_part_checksum(checksum_attrs, part_checksum_value)
    return part_checksum_value


def _validate_upload_part_checksum_headers(
        req, checksum_attrs):
    """
    Validate UploadPart checksum headers against the MPU configuration.

    FULL_OBJECT MPUs may omit a part checksum, in which case this function
    installs an input wrapper to calculate it. COMPOSITE MPUs require the
    request to supply the configured checksum algorithm.
    :param req: The S3 request.
    :param checksum_attrs: The checksum configuration stored on the MPU;
        must be checksum-aware (``is_checksum_aware`` is True).
    """
    request_checksum_attrs = MPUChecksumAttrs.from_request(req)
    if request_checksum_attrs.header:
        if request_checksum_attrs.header != checksum_attrs.header:
            raise InvalidRequest(
                # The type refers to the algorithm here, not the
                # x-amz-checksum-type value. This is the AWS
                # error message
                'Checksum Type mismatch occurred, expected checksum '
                'Type: %s, actual checksum Type: %s' % (
                    checksum_attrs.algorithm.lower(),
                    request_checksum_attrs.algorithm.lower()))
    elif checksum_attrs.is_full_object:
        checksum_hasher = get_checksum_hasher(checksum_attrs.header)
        # Since the request did not have a header, we have to add
        # the wrapper ourselves
        req.install_checksumming_input(
            checksum_hasher, checksum_attrs.header, None)
    else:
        raise InvalidRequest(
            'Checksum Type mismatch occurred, expected checksum '
            'Type: %s, actual checksum Type: null' %
            checksum_attrs.algorithm.lower())


class _CompositeChecksumHasher:
    """
    Computes the S3 composite checksum for ordered raw part checksum bytes.

    S3 composite checksum values hash the concatenated per-part checksum
    digests and append the part count to the base64 final digest.

    :param checksum_attrs: The checksum configuration that selects the
        algorithm.
    """
    def __init__(self, checksum_attrs):
        self.checksum_hasher = get_checksum_hasher(checksum_attrs.header)
        self.num_parts = 0

    def update(self, b64digest, *args, **kwargs):
        raw_value = validate_checksum_value(self.checksum_hasher, b64digest)
        self.checksum_hasher.update(raw_value)
        self.num_parts += 1

    def base64digest(self):
        """
        :return: The composite checksum value, including its -<part_count>
            suffix.
        """
        return '%s-%d' % (base64_str(self.checksum_hasher.digest()),
                          self.num_parts)

    def matches(self, b64digest):
        """
        :param b64digest: A client supplied checksum value, with or without a
            -<part_count> suffix. Its syntax has already been validated by
            ``_validate_complete_checksum_value``.
        :return: Whether the value matches the calculated composite checksum.
        """
        digest, part_count = split_composite_checksum(b64digest)
        if digest != base64_str(self.checksum_hasher.digest()):
            return False
        # S3 treats the part count suffix as optional.
        return part_count in (None, self.num_parts)


class _FullObjectChecksumHasher:
    """
    Computes the S3 full-object checksum from ordered part checksums and sizes.

    Full-object MPU checksums are CRC checksums over the concatenated object
    bytes. Since each part's CRC and size are known, we can
    combine the per-part CRC values.

    :param checksum_attrs: The checksum configuration that selects the CRC
        variant.
    """
    def __init__(self, checksum_attrs):
        self.checksum_hasher = get_checksum_hasher(checksum_attrs.header)

    def update(self, b64digest, length, *args, **kwargs):
        digest = validate_checksum_value(self.checksum_hasher, b64digest)
        crc = self.checksum_hasher.crc_from_digest(digest)
        self.checksum_hasher.combine(crc, length)

    def base64digest(self):
        return base64_str(self.checksum_hasher.digest())

    def matches(self, b64digest):
        """
        :param b64digest: A client supplied checksum value.
        :return: Whether the value matches the calculated full-object checksum.
        """
        return b64digest == self.base64digest()


def _get_upload_info(req, app, upload_id):
    """
    Make a HEAD request for existing upload object metadata. Tries the upload
    marker first, and then falls back to the manifest object.

    :param req: an S3Request object.
    :param app: the wsgi app.
    :param upload_id: the upload id.
    :returns: a tuple of (S3Response, boolean) where the boolean is True if the
        response is from the upload marker and False otherwise.
    :raises: NoSuchUpload if neither the marker nor the manifest were found.
    """

    container = req.container_name + MULTIUPLOAD_SUFFIX
    obj = '%s/%s' % (req.object_name, upload_id)

    # XXX: if we leave the copy-source header, somewhere later we might
    # drop in a ?version-id=... query string that's utterly inappropriate
    # for the upload marker. Until we get around to fixing that, just pop
    # it off for now...
    copy_source = req.headers.pop('X-Amz-Copy-Source', None)
    try:
        resp = req.get_response(app, 'HEAD', container=container, obj=obj)
        return resp, True
    except NoSuchKey:
        # ensure consistent path and policy are logged despite manifest HEAD
        upload_marker_path = req.environ.get('s3api.backend_path')
        policy_index = req.policy_index
        try:
            resp = req.get_response(app, 'HEAD')
            if resp.s3api_sysmeta_headers.get(
                    s3api_sysmeta_header('object', 'upload-id')) == upload_id:
                return resp, False
        except NoSuchKey:
            pass
        finally:
            # Ops often find it more useful for us to log the upload marker
            # path, so put it back
            if upload_marker_path is not None:
                req.environ['s3api.backend_path'] = upload_marker_path
            if policy_index is not None:
                req.policy_index = policy_index
        raise NoSuchUpload(upload_id=upload_id)
    finally:
        # ...making sure to restore any copy-source before returning
        if copy_source is not None:
            req.headers['X-Amz-Copy-Source'] = copy_source


def _make_complete_body(req, s3_etag, yielded_anything,
                        checksum_header=None, checksum_value=None,
                        checksum_type=None):
    """
    Build the CompleteMultipartUploadResult XML response body.

    When a checksum was persisted for the completed object, include the
    matching checksum element and checksum type in the AWS-compatible result.
    :param req: The S3 request.
    :param s3_etag: The final S3 multipart ETag without surrounding quotes.
    :param yielded_anything: Whether heartbeat output has already been sent.
    :param checksum_header: The checksum header used for the final checksum.
    :param checksum_value: The final checksum value to include.
    :param checksum_type: The S3 checksum type, such as COMPOSITE.
    :return: Serialized XML bytes.
    """
    result_elem = Element('CompleteMultipartUploadResult')

    # NOTE: boto with sig v4 appends port to HTTP_HOST value at
    # the request header when the port is non default value and it
    # makes req.host_url like as http://localhost:8080:8080/path
    # that obviously invalid. Probably it should be resolved at
    # swift.common.swob though, tentatively we are parsing and
    # reconstructing the correct host_url info here.
    # in detail, https://github.com/boto/boto/pull/3513
    parsed_url = urlparse(req.host_url)
    host_url = '%s://%s' % (parsed_url.scheme, parsed_url.hostname)
    # Why are we doing our own port parsing? Because py3 decided
    # to start raising ValueErrors on access after parsing such
    # an invalid port
    netloc = parsed_url.netloc.split('@')[-1].split(']')[-1]
    if ':' in netloc:
        port = netloc.split(':', 2)[1]
        host_url += ':%s' % port

    SubElement(result_elem, 'Location').text = host_url + req.path
    SubElement(result_elem, 'Bucket').text = req.container_name
    SubElement(result_elem, 'Key').text = wsgi_to_str(req.object_name)
    SubElement(result_elem, 'ETag').text = '"%s"' % s3_etag
    checksum_elem_name = checksum_xml_element_from_header(checksum_header)
    if checksum_elem_name and checksum_value:
        SubElement(result_elem, checksum_elem_name).text = checksum_value
    if checksum_type and checksum_value:
        SubElement(result_elem, 'ChecksumType').text = checksum_type
    body = tostring(result_elem, xml_declaration=not yielded_anything)
    if yielded_anything:
        return b'\n' + body
    return body


class _CompleteMultipartUploadSloManifestHook:
    """Validate an S3 multipart upload's SLO manifest."""
    ERROR_MESSAGE = 'Invalid S3 multipart upload manifest'

    def __init__(
            self, min_segment_size, upload_id, mpu_checksum_attrs,
            request_checksum_attrs, req_part_checksums):
        """Capture the request-local multipart completion state."""
        self.min_segment_size = min_segment_size
        self.upload_id = upload_id
        self.mpu_checksum_attrs = mpu_checksum_attrs
        self.request_checksum_attrs = request_checksum_attrs
        self.req_part_checksums = req_part_checksums
        self.final_checksum_value = mpu_checksum_attrs.value
        self._error = None

    @property
    def error(self):
        # the property facilitates easier test mocking
        return self._error

    def _create_error(self, error):
        """Cache an S3 error and return an SLO manifest error."""
        self._error = error
        return [(self.upload_id, self.ERROR_MESSAGE)]

    def _get_stored_part_checksum_value(self, manifest_item):
        """
        Return the checksum value found in the HEAD response headers included
        in an slo manifest item, or None if no checksum value is found.
        """
        segment_head_headers = manifest_item.get('headers') or {}
        stored_checksum_value = segment_head_headers.get(
            s3api_sysmeta_header('object', 'checksum-value'))
        return stored_checksum_value

    def _get_req_part_checksum_value(self, req_part_checksum):
        """Validate and return the checksum for a completed part."""
        req_checksum_value = req_part_checksum['checksum_value']

        # FULL_OBJECT completion may omit client-provided per-part
        # checksums; use the value persisted during UploadPart instead.
        # COMPOSITE parts require a checksum. If the client provided one,
        # make sure it matches the stored value.
        if req_checksum_value is None and self.mpu_checksum_attrs.is_composite:
            raise InvalidRequest(
                'The upload was created using a %s checksum. The complete '
                'request must include the checksum for each part. It was '
                'missing for part %d in the request.' % (
                    self.mpu_checksum_attrs.algorithm.lower(),
                    req_part_checksum['part_number']))
        return req_checksum_value

    def _validate_part_checksum(self, slo_manifest_item, req_part_checksum):
        stored_checksum_value = (
            self._get_stored_part_checksum_value(slo_manifest_item))
        if not stored_checksum_value:
            # No checksum is available for this part.
            return None

        req_checksum_value = self._get_req_part_checksum_value(
            req_part_checksum)
        if req_checksum_value and req_checksum_value != stored_checksum_value:
            # S3 does not return the invalid checksum in this error.
            raise InvalidPart(
                upload_id=self.upload_id,
                part_number=req_part_checksum['part_number'],
                e_tag=req_part_checksum['etag'])
        return stored_checksum_value

    def _validate_parts(self, slo_manifest):
        """
        Validate all manifest parts in S3 error-precedence order.

        :raises ErrorResponse: The highest-precedence segment error.
        """
        if self.mpu_checksum_attrs.is_checksum_aware:
            if self.mpu_checksum_attrs.is_composite:
                hasher = _CompositeChecksumHasher(self.mpu_checksum_attrs)
            else:
                hasher = _FullObjectChecksumHasher(self.mpu_checksum_attrs)
        else:
            hasher = None

        # Keep all part errors so S3 error precedence can select the response.
        missing_part_checksum = nonconsecutive = part_checksum_error = \
            size_error = False
        req_part_checksums = (
            self.req_part_checksums if hasher else repeat(None))
        for index, (slo_manifest_item, req_part_checksum) in enumerate(
                zip(slo_manifest, req_part_checksums)):
            if slo_manifest_item is None:
                # SLO reports this failed part HEAD.
                missing_part_checksum = True
                continue

            if hasher:
                if (self.mpu_checksum_attrs.is_composite and
                        req_part_checksum['part_number'] != index + 1):
                    # COMPOSITE requires consecutive parts. Save the error
                    # because other client errors can take precedence.
                    nonconsecutive = True

                part_checksum_invalid = False
                try:
                    part_checksum_value = self._validate_part_checksum(
                        slo_manifest_item, req_part_checksum)
                except ErrorResponse as err:
                    # Save the error, but use the stored checksum
                    # when calculating the final checksum.
                    part_checksum_error = err
                    part_checksum_invalid = True
                    part_checksum_value = (
                        self._get_stored_part_checksum_value(
                            slo_manifest_item))

                # Calculate the final checksum to preserve error precedence.
                if part_checksum_value:
                    hasher.update(part_checksum_value,
                                  slo_manifest_item['bytes'])
                else:
                    # A part without a stored checksum prevents the final
                    # checksum calculation.
                    missing_part_checksum = True

                if part_checksum_invalid:
                    # A part checksum error takes precedence over a part
                    # size error.
                    continue

            if (index < len(slo_manifest) - 1 and
                    slo_manifest_item['bytes'] < self.min_segment_size):
                # Save the size error and check the remaining parts.
                size_error = size_error or EntityTooSmall()

        # Preserve S3 error precedence: final checksum, size, part checksum,
        # then nonconsecutive parts for COMPOSITE.
        if (hasher
                and not missing_part_checksum
                and self.request_checksum_attrs.header
                and not hasher.matches(
                    self.request_checksum_attrs.value)):
            raise BadDigest(
                'The %s you specified did not match the calculated '
                'checksum.' %
                self.request_checksum_attrs.algorithm.lower())
        elif size_error:
            raise size_error
        elif part_checksum_error:
            raise part_checksum_error
        elif missing_part_checksum:
            return None
        elif nonconsecutive:
            # S3 returns a 500 for nonconsecutive parts with COMPOSITE checksum
            # type, so we do the same:
            # https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#using-additional-checksums
            raise InternalError()
        elif hasher:
            return hasher.base64digest()
        return None

    def __call__(self, manifest):
        """Validate the manifest and calculate its completed checksum."""
        try:
            self.final_checksum_value = self._validate_parts(manifest)
        except ErrorResponse as err:
            # Don't let an S3 error escape from inside the SLO response
            # iterator. Ask SLO to fail the manifest, then replay the error in
            # the S3API response iterator.
            return self._create_error(err)

        return None


class PartController(Controller):
    """
    Handles the following APIs:

    * Upload Part
    * Upload Part - Copy

    Those APIs are logged as PART operations in the S3 server log.
    """
    acl_handler = PartAclHandler
    resource_type = 'PART'

    @public
    @object_operation
    @check_container_existence
    def PUT(self, req):
        """
        Handles Upload Part and Upload Part Copy.
        """

        if 'uploadId' not in req.params:
            raise InvalidArgument('ResourceType', 'partNumber',
                                  'Unexpected query string parameter')

        part_number = req.validate_part_number()

        upload_id = get_param(req, 'uploadId')
        upload_info, is_marker = _get_upload_info(req, self.app, upload_id)
        mpu_checksum_attrs = MPUChecksumAttrs.from_sysmeta(
            upload_info.sysmeta_headers, is_marker)
        if 'X-Amz-Copy-Source' not in req.headers:
            if mpu_checksum_attrs.is_checksum_aware:
                # S3Request has validated that the request's checksum headers
                # are self-consistent, now we validate them for consistency
                # with the stored MPU checksum attributes
                _validate_upload_part_checksum_headers(
                    req, mpu_checksum_attrs)
                # Legacy MPUs validate input but do not store part checksums.
                req.enable_checksum_persistence()

        req.container_name += MULTIUPLOAD_SUFFIX
        req.object_name = '%s/%s/%d' % (req.object_name, upload_id,
                                        part_number)

        source_resp = req.check_copy_source(self.app)
        if 'X-Amz-Copy-Source' in req.headers and \
                'X-Amz-Copy-Source-Range' in req.headers:
            rng = req.headers['X-Amz-Copy-Source-Range']

            header_valid = True
            try:
                rng_obj = Range(rng)
                if len(rng_obj.ranges) != 1:
                    header_valid = False
            except ValueError:
                header_valid = False
            if not header_valid:
                err_msg = ('The x-amz-copy-source-range value must be of the '
                           'form bytes=first-last where first and last are '
                           'the zero-based offsets of the first and last '
                           'bytes to copy')
                raise InvalidArgument('x-amz-source-range', rng, err_msg)

            source_size = int(source_resp.headers['Content-Length'])
            if not rng_obj.ranges_for_length(source_size):
                err_msg = ('Range specified is not valid for source object '
                           'of size: %s' % source_size)
                raise InvalidArgument('x-amz-source-range', rng, err_msg)

            req.headers['Range'] = rng
            del req.headers['X-Amz-Copy-Source-Range']

        resp = req.get_response(self.app)

        if req.checksum_sysmeta_footers:
            resp.headers.update(checksum_sysmeta_to_response_headers(
                req.checksum_sysmeta_footers))
            # UploadPart responses do not echo the type
            resp.headers.pop('x-amz-checksum-type', None)
        if 'X-Amz-Copy-Source' in req.headers:
            last_modified_ts = S3Timestamp(
                parse_date_header(resp.headers['Last-Modified']))
            resp.append_copy_resp_body('CopyPartResult',
                                       last_modified_ts.s3xmlformat)

        resp.status = 200
        return resp


class UploadsController(Controller):
    """
    Handles the following APIs:

    * List Multipart Uploads
    * Initiate Multipart Upload

    Those APIs are logged as UPLOADS operations in the S3 server log.
    """
    acl_handler = UploadsAclHandler
    resource_type = 'UPLOADS'

    def _validated_utf8_param(self, req, name, default=None):
        """
        Return a request parameter or raise an S3 InvalidArgument error.
        """
        value = wsgi_to_str(req.params.get(name, default))
        if value is None:
            return None
        if value and not constraints.check_utf8(value):
            raise InvalidArgument(name, wsgi_quote(req.params[name]))
        return value

    @public
    @bucket_operation(err_resp=InvalidRequest,
                      err_msg="Key is not expected for the GET method "
                              "?uploads subresource")
    @check_container_existence
    def GET(self, req):
        """
        Handles List Multipart Uploads
        """

        def separate_uploads(uploads, prefix, delimiter):
            """
            separate_uploads will separate uploads into non_delimited_uploads
            (a subset of uploads) and common_prefixes according to the
            specified delimiter. non_delimited_uploads is a list of uploads
            which exclude the delimiter. common_prefixes is a set of prefixes
            prior to the specified delimiter. Note that the prefix in the
            common_prefixes includes the delimiter itself.

            i.e. if '/' delimiter specified and then the uploads is consists of
            ['foo', 'foo/bar'], this function will return (['foo'], ['foo/']).

            :param uploads: A list of uploads dictionary
            :param prefix: A string of prefix reserved on the upload path.
                           (i.e. the delimiter must be searched behind the
                            prefix)
            :param delimiter: A string of delimiter to split the path in each
                              upload

            :return (non_delimited_uploads, common_prefixes)
            """
            non_delimited_uploads = []
            common_prefixes = set()
            for upload in uploads:
                key = upload['key']
                end = key.find(delimiter, len(prefix))
                if end >= 0:
                    common_prefix = key[:end + len(delimiter)]
                    common_prefixes.add(common_prefix)
                else:
                    non_delimited_uploads.append(upload)
            return non_delimited_uploads, sorted(common_prefixes)

        encoding_type = self._validated_utf8_param(req, 'encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        keymarker = self._validated_utf8_param(req, 'key-marker', '')
        uploadid = self._validated_utf8_param(req, 'upload-id-marker', '')
        prefix = self._validated_utf8_param(req, 'prefix', '')
        delimiter = self._validated_utf8_param(req, 'delimiter')
        maxuploads = req.get_validated_param(
            'max-uploads', DEFAULT_MAX_UPLOADS, DEFAULT_MAX_UPLOADS)

        query = {
            'format': 'json',
            'marker': '',
        }

        if uploadid and keymarker:
            query.update({'marker': '%s/%s' % (keymarker, uploadid)})
        elif keymarker:
            query.update({'marker': '%s/~' % (keymarker)})
        if 'prefix' in req.params:
            query.update({'prefix': prefix})

        container = req.container_name + MULTIUPLOAD_SUFFIX
        uploads = []
        prefixes = []

        def object_to_upload(object_info):
            obj, upid = object_info['name'].rsplit('/', 1)
            obj_dict = {'key': obj,
                        'upload_id': upid,
                        'last_modified': object_info['last_modified']}
            return obj_dict

        is_segment = re.compile('.*/[0-9]+$')

        while len(uploads) < maxuploads:
            try:
                resp = req.get_response(self.app, container=container,
                                        query=query)
                objects = json.loads(resp.body)
            except NoSuchBucket:
                # Assume NoSuchBucket as no uploads
                objects = []
            if not objects:
                break

            new_uploads = [object_to_upload(obj) for obj in objects
                           if not is_segment.match(obj.get('name', ''))]
            new_prefixes = []
            if 'delimiter' in req.params:
                new_uploads, new_prefixes = separate_uploads(
                    new_uploads, prefix, delimiter)
            uploads.extend(new_uploads)
            prefixes.extend(new_prefixes)
            query['marker'] = objects[-1]['name']

        truncated = len(uploads) >= maxuploads
        if len(uploads) > maxuploads:
            uploads = uploads[:maxuploads]

        nextkeymarker = ''
        nextuploadmarker = ''
        if len(uploads) > 1:
            nextuploadmarker = uploads[-1]['upload_id']
            nextkeymarker = uploads[-1]['key']

        result_elem = Element('ListMultipartUploadsResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'KeyMarker').text = keymarker
        SubElement(result_elem, 'UploadIdMarker').text = uploadid
        SubElement(result_elem, 'NextKeyMarker').text = nextkeymarker
        SubElement(result_elem, 'NextUploadIdMarker').text = nextuploadmarker
        if 'delimiter' in req.params:
            SubElement(result_elem, 'Delimiter').text = delimiter
        if 'prefix' in req.params:
            SubElement(result_elem, 'Prefix').text = prefix
        SubElement(result_elem, 'MaxUploads').text = str(maxuploads)
        if encoding_type is not None:
            SubElement(result_elem, 'EncodingType').text = encoding_type
        SubElement(result_elem, 'IsTruncated').text = \
            'true' if truncated else 'false'

        # TODO: don't show uploads which are initiated before this bucket is
        # created.
        for u in uploads:
            upload_elem = SubElement(result_elem, 'Upload')
            name = u['key']
            if encoding_type == 'url':
                name = quote(name)
            SubElement(upload_elem, 'Key').text = name
            SubElement(upload_elem, 'UploadId').text = u['upload_id']
            initiator_elem = SubElement(upload_elem, 'Initiator')
            SubElement(initiator_elem, 'ID').text = req.user_id
            SubElement(initiator_elem, 'DisplayName').text = req.user_id
            owner_elem = SubElement(upload_elem, 'Owner')
            SubElement(owner_elem, 'ID').text = req.user_id
            SubElement(owner_elem, 'DisplayName').text = req.user_id
            SubElement(upload_elem, 'StorageClass').text = 'STANDARD'
            SubElement(upload_elem, 'Initiated').text = \
                S3Timestamp.from_isoformat(u['last_modified']).s3xmlformat

        for p in prefixes:
            elem = SubElement(result_elem, 'CommonPrefixes')
            SubElement(elem, 'Prefix').text = p

        body = tostring(result_elem)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Initiate Multipart Upload.
        """
        if len(req.object_name) > constraints.MAX_OBJECT_NAME_LENGTH:
            # Note that we can still run into trouble where the MPU is just
            # within the limit, which means the segment names will go over
            raise KeyTooLongError()

        # Create a unique S3 upload id from UUID to avoid duplicates.
        upload_id = unique_id()

        mpu_checksum_attrs = MPUChecksumAttrs.from_request(req)
        _validate_mpu_initiate_checksum_headers(mpu_checksum_attrs)
        if self.conf.allow_store_checksums and \
                mpu_checksum_attrs.is_checksum_aware:
            req.headers.update(checksum_sysmeta_headers(
                mpu_checksum_attrs.algorithm,
                None,
                mpu_checksum_attrs.type))

        seg_container = req.container_name + MULTIUPLOAD_SUFFIX
        content_type = req.headers.get('Content-Type')
        if content_type:
            req.headers[
                s3api_sysmeta_header('object', 'has-content-type')] = 'yes'
            req.headers[
                s3api_sysmeta_header('object', 'content-type')] = content_type
        else:
            req.headers[
                s3api_sysmeta_header('object', 'has-content-type')] = 'no'
        req.headers['Content-Type'] = 'application/directory'

        try:
            seg_req = copy.copy(req)
            seg_req.environ = copy.copy(req.environ)
            seg_req.container_name = seg_container
            seg_req.get_container_info(self.app)
        except NoSuchBucket:
            try:
                # multi-upload bucket doesn't exist, create one with
                # same storage policy and acls as the primary bucket
                info = req.get_container_info(self.app)
                policy_name = POLICIES[info['storage_policy']].name
                hdrs = {'X-Storage-Policy': policy_name}
                if info.get('read_acl'):
                    hdrs['X-Container-Read'] = info['read_acl']
                if info.get('write_acl'):
                    hdrs['X-Container-Write'] = info['write_acl']
                seg_req.get_response(self.app, 'PUT', seg_container, '',
                                     headers=hdrs)
            except (BucketAlreadyExists, BucketAlreadyOwnedByYou):
                pass

        obj = '%s/%s' % (req.object_name, upload_id)

        req.headers.pop('Etag', None)
        req.headers.pop('Content-Md5', None)

        req.get_response(self.app, 'PUT', seg_container, obj, body='')

        result_elem = Element('InitiateMultipartUploadResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        SubElement(result_elem, 'Key').text = wsgi_to_str(req.object_name)
        SubElement(result_elem, 'UploadId').text = upload_id

        body = tostring(result_elem)

        resp = HTTPOk(body=body, content_type='application/xml')
        if self.conf.allow_store_checksums and \
                mpu_checksum_attrs.is_checksum_aware:
            resp.headers.update({
                'x-amz-checksum-algorithm':
                    mpu_checksum_attrs.algorithm,
                'x-amz-checksum-type': mpu_checksum_attrs.type,
            })
        return resp


class UploadController(Controller):
    """
    Handles the following APIs:

    * List Parts
    * Abort Multipart Upload
    * Complete Multipart Upload

    Those APIs are logged as UPLOAD operations in the S3 server log.
    """
    acl_handler = UploadAclHandler
    resource_type = 'UPLOAD'

    @public
    @object_operation
    @check_container_existence
    def GET(self, req):
        """
        Handles List Parts.
        """
        def filter_part_num_marker(o):
            try:
                num = int(os.path.basename(o['name']))
                return num > part_num_marker
            except ValueError:
                return False

        encoding_type = get_param(req, 'encoding-type')
        if encoding_type is not None and encoding_type != 'url':
            err_msg = 'Invalid Encoding Method specified in Request'
            raise InvalidArgument('encoding-type', encoding_type, err_msg)

        upload_id = get_param(req, 'uploadId')
        _get_upload_info(req, self.app, upload_id)

        maxparts = req.get_validated_param(
            'max-parts', DEFAULT_MAX_PARTS_LISTING,
            self.conf.max_parts_listing)
        part_num_marker = req.get_validated_param(
            'part-number-marker', 0)

        object_name = wsgi_to_str(req.object_name)
        query = {
            'format': 'json',
            'prefix': '%s/%s/' % (object_name, upload_id),
            'delimiter': '/',
            'marker': '',
        }

        container = req.container_name + MULTIUPLOAD_SUFFIX
        # Because the parts are out of order in Swift, we list up to the
        # maximum number of parts and then apply the marker and limit options.
        objects = []
        while True:
            resp = req.get_response(self.app, container=container, obj='',
                                    query=query)
            new_objects = json.loads(resp.body)
            if not new_objects:
                break
            objects.extend(new_objects)
            query['marker'] = new_objects[-1]['name']

        last_part = 0

        # If the caller requested a list starting at a specific part number,
        # construct a sub-set of the object list.
        objList = [obj for obj in objects if filter_part_num_marker(obj)]

        # pylint: disable-msg=E1103
        objList.sort(key=lambda o: int(o['name'].split('/')[-1]))

        if len(objList) > maxparts:
            objList = objList[:maxparts]
            truncated = True
        else:
            truncated = False
        # TODO: We have to retrieve object list again when truncated is True
        # and some objects filtered by invalid name because there could be no
        # enough objects for limit defined by maxparts.

        if objList:
            o = objList[-1]
            last_part = os.path.basename(o['name'])

        result_elem = Element('ListPartsResult')
        SubElement(result_elem, 'Bucket').text = req.container_name
        if encoding_type == 'url':
            object_name = quote(object_name)
        SubElement(result_elem, 'Key').text = object_name
        SubElement(result_elem, 'UploadId').text = upload_id

        initiator_elem = SubElement(result_elem, 'Initiator')
        SubElement(initiator_elem, 'ID').text = req.user_id
        SubElement(initiator_elem, 'DisplayName').text = req.user_id
        owner_elem = SubElement(result_elem, 'Owner')
        SubElement(owner_elem, 'ID').text = req.user_id
        SubElement(owner_elem, 'DisplayName').text = req.user_id

        SubElement(result_elem, 'StorageClass').text = 'STANDARD'
        SubElement(result_elem, 'PartNumberMarker').text = str(part_num_marker)
        SubElement(result_elem, 'NextPartNumberMarker').text = str(last_part)
        SubElement(result_elem, 'MaxParts').text = str(maxparts)
        if 'encoding-type' in req.params:
            SubElement(result_elem, 'EncodingType').text = \
                get_param(req, 'encoding-type')
        SubElement(result_elem, 'IsTruncated').text = \
            'true' if truncated else 'false'

        for i in objList:
            part_elem = SubElement(result_elem, 'Part')
            SubElement(part_elem, 'PartNumber').text = i['name'].split('/')[-1]
            SubElement(part_elem, 'LastModified').text = \
                S3Timestamp.from_isoformat(i['last_modified']).s3xmlformat
            SubElement(part_elem, 'ETag').text = '"%s"' % i['hash']
            SubElement(part_elem, 'Size').text = str(i['bytes'])

        body = tostring(result_elem)

        return HTTPOk(body=body, content_type='application/xml')

    @public
    @object_operation
    @check_container_existence
    def DELETE(self, req):
        """
        Handles Abort Multipart Upload.
        """
        upload_id = get_param(req, 'uploadId')
        _get_upload_info(req, self.app, upload_id)

        # First check to see if this multi-part upload was already
        # completed.  Look in the primary container, if the object exists,
        # then it was completed and we return an error here.
        container = req.container_name + MULTIUPLOAD_SUFFIX
        obj = '%s/%s' % (req.object_name, upload_id)
        req.get_response(self.app, container=container, obj=obj)

        # The completed object was not found so this
        # must be a multipart upload abort.
        # We must delete any uploaded segments for this UploadID and then
        # delete the object in the main container as well
        object_name = wsgi_to_str(req.object_name)
        query = {
            'format': 'json',
            'prefix': '%s/%s/' % (object_name, upload_id),
            'delimiter': '/',
        }

        resp = req.get_response(self.app, 'GET', container, '', query=query)

        #  Iterate over the segment objects and delete them individually
        objects = json.loads(resp.body)
        while objects:
            for o in objects:
                container = req.container_name + MULTIUPLOAD_SUFFIX
                obj = bytes_to_wsgi(o['name'].encode('utf-8'))
                req.get_response(self.app, container=container, obj=obj)
            query['marker'] = objects[-1]['name']
            resp = req.get_response(self.app, 'GET', container, '',
                                    query=query)
            objects = json.loads(resp.body)

        return HTTPNoContent()

    @public
    @object_operation
    @check_container_existence
    def POST(self, req):
        """
        Handles Complete Multipart Upload.
        """
        upload_id = get_param(req, 'uploadId')
        # Check for conditional requests before getting upload info so the
        # headers can't bleed into the HEAD
        if req.headers.get('If-None-Match', '*') != '*' or any(
                h in req.headers for h in (
                    'If-Match', 'If-Modified-Since', 'If-Unmodified-Since')):
            raise S3NotImplemented(
                'Conditional uploads are not supported.')

        resp, is_marker = _get_upload_info(req, self.app, upload_id)
        request_checksum_attrs = MPUChecksumAttrs.from_request(req)
        mpu_checksum_attrs = MPUChecksumAttrs.from_sysmeta(
            resp.sysmeta_headers, is_marker)
        _validate_mpu_complete_checksum_headers(
            request_checksum_attrs, mpu_checksum_attrs)

        marker_delta = (
            float(Timestamp(resp.sw_headers.get('X-Backend-Timestamp', '0')))
            - float(NormalTimestamp.now()))
        if is_marker and marker_delta >= 0:
            # Somehow the marker was created in the future w.r.t. this thread's
            # clock. The manifest PUT may succeed but the subsequent marker
            # DELETE will fail, so don't attempt either.
            self.logger.error('Unable to Complete Multipart Upload,'
                              ' marker is %0.5fs newer', marker_delta)
            raise ServiceUnavailable(reason='mpu_clock_skew')

        headers = {'Accept': 'application/json',
                   s3api_sysmeta_header('object', 'upload-id'): upload_id}
        for key, val in resp.headers.items():
            _key = key.lower()
            if _key.startswith('x-amz-meta-'):
                headers['x-object-meta-' + _key[11:]] = val
            elif _key in ('content-encoding', 'content-language',
                          'content-disposition', 'expires', 'cache-control'):
                headers[key] = val

        hct_header = s3api_sysmeta_header('object', 'has-content-type')
        if resp.s3api_sysmeta_headers.get(hct_header) == 'yes':
            content_type = resp.s3api_sysmeta_headers.get(
                s3api_sysmeta_header('object', 'content-type'))
        elif hct_header in resp.s3api_sysmeta_headers:
            # has-content-type is present but false, so no content type was
            # set on initial upload. In that case, we won't set one on our
            # PUT request. Swift will end up guessing one based on the
            # object name.
            content_type = None
        else:
            content_type = resp.headers.get('Content-Type')

        if content_type:
            headers['Content-Type'] = content_type

        container = req.container_name + MULTIUPLOAD_SUFFIX
        s3_etag_hasher = md5(usedforsecurity=False)
        manifest = []
        req_part_checksums = []
        previous_number = 0
        try:
            xml = req.xml(MAX_COMPLETE_UPLOAD_BODY_SIZE)
            if not xml:
                raise InvalidRequest(msg='You must specify at least one part')
            # If an MD5 was provided, we need to verify it.
            if req.check_md5(xml):
                # We're only interested in the body here, in the
                # multipart-upload controller -- *don't* let etag get
                # plumbed down to the object-server
                req.headers.pop('etag', None)

            complete_elem = fromstring(
                xml, 'CompleteMultipartUpload', self.logger)
            for part_elem in complete_elem.iterchildren('Part'):
                part_number = int(part_elem.find('./PartNumber').text)
                if part_number <= previous_number:
                    raise InvalidPartOrder(upload_id=upload_id)
                previous_number = part_number

                etag = normalize_etag(part_elem.find('./ETag').text)
                if etag is None:
                    raise InvalidPart(upload_id=upload_id,
                                      part_number=part_number,
                                      e_tag=etag)
                if len(etag) != 32 or any(c not in '0123456789abcdef'
                                          for c in etag):
                    raise InvalidPart(upload_id=upload_id,
                                      part_number=part_number,
                                      e_tag=etag)
                checksum_value = _validate_request_manifest_checksum_part(
                    part_elem, mpu_checksum_attrs, part_number)
                if mpu_checksum_attrs.is_checksum_aware:
                    req_part_checksums.append({
                        'part_number': part_number,
                        'checksum_value': checksum_value,
                        'etag': etag,
                    })
                manifest.append({
                    'path': '/%s/%s/%s/%d' % (
                        wsgi_to_str(container), wsgi_to_str(req.object_name),
                        upload_id, part_number),
                    'etag': etag})
                s3_etag_hasher.update(binascii.a2b_hex(etag))
        except (XMLSyntaxError, DocumentInvalid):
            # NB: our schema definitions catch uploads with no parts here
            raise MalformedXML()
        except ErrorResponse:
            raise
        except Exception as e:
            self.logger.error(e)
            raise

        s3_etag = '%s-%d' % (s3_etag_hasher.hexdigest(), len(manifest))
        s3_etag_header = s3api_sysmeta_header('object', 'etag')
        # This header should only already be present if the upload marker
        # has been cleaned up and the current target uses the same upload-id
        already_uploaded_s3_etag = resp.s3api_sysmeta_headers.get(
            s3_etag_header)
        if already_uploaded_s3_etag == s3_etag:
            # If the segments to use haven't changed, the work is already done.
            # S3 omits the checksum elements when replaying a completed upload.
            return HTTPOk(body=_make_complete_body(req, s3_etag, False),
                          content_type='application/xml')
        elif already_uploaded_s3_etag:
            # If the header's present but *doesn't* match, upload-id is
            # no longer valid
            raise NoSuchUpload(upload_id=upload_id)
        headers[s3_etag_header] = s3_etag
        # Leave base header value blank; SLO will populate
        headers[get_container_update_override_key('etag')] = \
            serialize_header('', {'s3_etag': s3_etag})

        # Install the SLO manifest hook
        manifest_hook = _CompleteMultipartUploadSloManifestHook(
            min_segment_size=self.conf.min_segment_size,
            upload_id=upload_id,
            mpu_checksum_attrs=mpu_checksum_attrs,
            request_checksum_attrs=request_checksum_attrs,
            req_part_checksums=req_part_checksums)
        req.environ['swift.callback.slo_manifest_hook'] = manifest_hook

        # SLO invokes the manifest hook before issuing the downstream manifest
        # PUT. By the time the object controller requests footers for that PUT,
        # the hook has calculated the final checksum metadata.
        if self.conf.allow_store_checksums and \
                mpu_checksum_attrs.is_checksum_aware:
            original_update_footers = req.environ.get(
                'swift.callback.update_footers')

            def update_footers(footers):
                """Persist final MPU checksum metadata, when available."""
                if original_update_footers:
                    original_update_footers(footers)
                checksum_value = manifest_hook.final_checksum_value
                if checksum_value:
                    footers.update(checksum_sysmeta_headers(
                        mpu_checksum_attrs.algorithm,
                        checksum_value,
                        mpu_checksum_attrs.type))
            req.environ['swift.callback.update_footers'] = update_footers

        start_time = time.time()

        def response_iter():
            # NB: XML requires that the XML declaration, if present, be at the
            # very start of the document. Clients *will* call us out on not
            # being valid XML if we pass through whitespace before it.
            # Track whether we've sent anything yet so we can yield out that
            # declaration *first*
            yielded_anything = False

            try:
                try:
                    # TODO: add support for versioning
                    put_resp = req.get_response(
                        self.app, 'PUT', body=json.dumps(manifest),
                        query={'multipart-manifest': 'put',
                               'heartbeat': 'on'},
                        headers=headers)
                    if put_resp.status_int == 202:
                        body = []
                        put_resp.fix_conditional_response()
                        for chunk in put_resp.response_iter:
                            if not chunk.strip():
                                if time.time() - start_time < 10:
                                    # Include some grace period to keep
                                    # ceph-s3tests happy
                                    continue
                                if not yielded_anything:
                                    yield (b'<?xml version="1.0" '
                                           b'encoding="UTF-8"?>\n')
                                yielded_anything = True
                                yield chunk
                                continue
                            body.append(chunk)
                        body = json.loads(b''.join(body))
                        if body['Response Status'] == \
                                '412 Precondition Failed':
                            raise PreconditionFailed
                        elif body['Response Status'] != '201 Created':
                            for seg, err in body['Errors']:
                                if err == manifest_hook.ERROR_MESSAGE:
                                    raise manifest_hook.error
                                elif err in ('Etag Mismatch', '404 Not Found'):
                                    raise InvalidPart(upload_id=upload_id)
                            raise InvalidRequest(
                                status=body['Response Status'],
                                msg='\n'.join(': '.join(err)
                                              for err in body['Errors']))
                except InvalidRequest as err_resp:
                    msg = err_resp._msg
                    if manifest_hook.ERROR_MESSAGE in msg:
                        raise manifest_hook.error
                    elif ', Etag Mismatch' in msg:
                        raise InvalidPart(upload_id=upload_id)
                    elif ', 404 Not Found' in msg:
                        raise InvalidPart(upload_id=upload_id)
                    else:
                        raise

                # clean up the multipart-upload record
                obj = '%s/%s' % (req.object_name, upload_id)
                try:
                    req.get_response(self.app, 'DELETE', container, obj)
                except NoSuchKey:
                    # The important thing is that we wrote out a tombstone to
                    # make sure the marker got cleaned up. If it's already
                    # gone (e.g., because of concurrent completes or a retried
                    # complete), so much the better.
                    pass

                checksum_value = (
                    manifest_hook.final_checksum_value
                    if self.conf.allow_store_checksums else None)
                yield _make_complete_body(
                    req, s3_etag, yielded_anything,
                    mpu_checksum_attrs.header,
                    checksum_value,
                    mpu_checksum_attrs.type)
            except ErrorResponse as err_resp:
                if yielded_anything:
                    err_resp.xml_declaration = False
                    yield b'\n'
                else:
                    # Oh good, we can still change HTTP status code, too!
                    resp.status = err_resp.status
                for chunk in err_resp({}, lambda *a: None):
                    yield chunk

        resp = HTTPOk()  # assume we're good for now... but see above!
        resp.app_iter = reiterate(response_iter())
        resp.content_type = "application/xml"

        return resp
