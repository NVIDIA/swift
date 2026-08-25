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

from hashlib import sha1, sha256

from swift.common.header_key_dict import HeaderKeyDict
from swift.common.middleware.s3api.exception import \
    S3InputChecksumMismatch, S3InputChecksumTrailerInvalid
from swift.common.middleware.s3api.utils import s3api_sysmeta_header
from swift.common.utils import checksum, InputProxy, strict_b64decode, \
    base64_str


CHECKSUM_TYPE_FULL_OBJECT = 'FULL_OBJECT'
CHECKSUM_TYPE_COMPOSITE = 'COMPOSITE'


CHECKSUMS_BY_HEADER = {
    'x-amz-checksum-crc32': checksum.crc32,
    'x-amz-checksum-crc32c': checksum.crc32c,
    'x-amz-checksum-crc64nvme': checksum.crc64nvme,
    'x-amz-checksum-sha1': sha1,
    'x-amz-checksum-sha256': sha256,
}

CHECKSUM_XML_ELEMENTS = {
    'x-amz-checksum-crc32': 'ChecksumCRC32',
    'x-amz-checksum-crc32c': 'ChecksumCRC32C',
    'x-amz-checksum-crc64nvme': 'ChecksumCRC64NVME',
    'x-amz-checksum-sha1': 'ChecksumSHA1',
    'x-amz-checksum-sha256': 'ChecksumSHA256',
}


def normalize_checksum_mode(mode):
    """Return an uppercase x-amz-checksum-mode value."""
    if mode is not None:
        mode = mode.upper()
    return mode


def normalize_checksum_algorithm(algorithm):
    """Return a normalized S3 checksum algorithm name."""
    # Only case is insignificant: S3 rejects hyphenated spellings such as
    # SHA-256 and CRC-32C with InvalidRequest.
    return algorithm.strip().upper()


def normalize_checksum_type(checksum_type):
    """Return a normalized S3 checksum type."""
    return checksum_type.upper()


def checksum_header_from_algorithm(algorithm):
    """Return the canonical checksum header for an S3 algorithm."""
    return 'x-amz-checksum-%s' % normalize_checksum_algorithm(
        algorithm).lower()


def checksum_algorithm_from_header(checksum_header):
    """Return the normalized S3 algorithm from a checksum header."""
    return normalize_checksum_algorithm(
        checksum_header[len('x-amz-checksum-'):])


def format_supported_checksum_algorithms():
    """Return supported checksum algorithms for user-facing messages."""
    return '[%s]' % ', '.join(
        checksum_algorithm_from_header(checksum_header)
        for checksum_header in CHECKSUMS_BY_HEADER)


# FULL OBJECT checksums are only CRC algorithms
FULL_OBJECT_MPU_CHECKSUM_HEADERS = tuple(
    checksum_header for checksum_header in CHECKSUMS_BY_HEADER
    if checksum_algorithm_from_header(checksum_header).startswith('CRC'))

# COMPOSITE checksums are all algorithms except CRC64NVME
COMPOSITE_MPU_CHECKSUM_HEADERS = tuple(
    checksum_header for checksum_header in CHECKSUMS_BY_HEADER
    if checksum_algorithm_from_header(checksum_header) != 'CRC64NVME')


def checksum_xml_element_from_header(checksum_header):
    """Return the S3 XML element for a stored checksum header."""
    if checksum_header is None:
        return None
    return CHECKSUM_XML_ELEMENTS.get(checksum_header.lower())


def get_checksum_hasher(header):
    """
    Return a checksum hasher for an x-amz-checksum-* header.

    :raises KeyError: if the header does not name a supported algorithm.
    """
    return CHECKSUMS_BY_HEADER[header]()


def validate_checksum_value(checksum_hasher, b64digest):
    """Decode a checksum value with the exact size required by its hasher."""
    return strict_b64decode(
        b64digest,
        exact_size=checksum_hasher.digest_size,
    )


def split_composite_checksum(checksum_value):
    """
    Split a composite checksum value into its digest and part count.

    :param checksum_value: A composite checksum value, with or without a
        -<part_count> suffix.
    :return: A ``(b64digest, part_count)`` pair, where ``part_count`` is
        ``None`` if the value carries no suffix.
    :raises ValueError: if the digest is empty, or the suffix is not a
        positive integer.
    """
    # Base64 has no '-', so the first one always starts the suffix.
    b64digest, sep, part_count = checksum_value.partition('-')
    if not b64digest:
        raise ValueError('missing digest')
    if not sep:
        return b64digest, None
    if not part_count.isdigit() or int(part_count) < 1:
        raise ValueError('invalid part count %r' % part_count)
    return b64digest, int(part_count)


def checksum_sysmeta_headers(checksum_algorithm, checksum_value,
                             checksum_type):
    """Return s3api sysmeta headers for given checksum metadata.

    :return: a HeaderKeyDict; keys with value None are omitted.
    """
    return HeaderKeyDict({
        s3api_sysmeta_header('object', 'checksum-algorithm'):
            normalize_checksum_algorithm(checksum_algorithm),
        # The registered value is encrypted when encryption is enabled.
        s3api_sysmeta_header('object', 'checksum-value'):
            checksum_value,
        s3api_sysmeta_header('object', 'checksum-type'): checksum_type,
    })


def checksum_sysmeta_values(sysmeta_headers):
    """Return the checksum algorithm, value, and type from object sysmeta."""
    return (
        sysmeta_headers.get(
            s3api_sysmeta_header('object', 'checksum-algorithm')),
        sysmeta_headers.get(
            s3api_sysmeta_header('object', 'checksum-value')),
        sysmeta_headers.get(
            s3api_sysmeta_header('object', 'checksum-type')),
    )


def checksum_sysmeta_to_response_headers(sysmeta_headers):
    """Translate complete checksum sysmeta into S3 response headers."""
    checksum_algorithm, checksum_value, checksum_type = \
        checksum_sysmeta_values(sysmeta_headers)
    if all([checksum_algorithm, checksum_value, checksum_type]):
        checksum_header = checksum_header_from_algorithm(checksum_algorithm)
        return {checksum_header: checksum_value,
                'x-amz-checksum-type': checksum_type}
    # object may have been created before checksum persistence was supported...
    return {}


class ChecksummingInput(InputProxy):
    """
    wsgi.input wrapper to calculate the X-Amz-Checksum-* of the input as it's
    read. The calculated value is checked against an expected value that is
    sent in either the request headers or trailers. To allow for the latter,
    the expected value is lazy fetched once the input has been read. If no
    checksum source is provided, the calculated value is retained without
    validation.

    :param wsgi_input: file-like object to be wrapped.
    :param content_length: the expected number of bytes to be read, or None
        if the input length is unknown and ``checksum_source`` is None.
    :param checksum_hasher: a hasher to calculate the checksum of read bytes.
    :param checksum_key: the name of the header or trailer that will have
        the expected checksum value to be checked.
    :param checksum_source: a dict that will have the ``checksum_key``, or
        None to retain the calculated checksum without validation.
    """

    def __init__(self, wsgi_input, content_length, checksum_hasher,
                 checksum_key, checksum_source):
        """Initialize a checksum-calculating input wrapper."""
        super().__init__(wsgi_input)
        self._expected_length = content_length
        self._checksum_hasher = checksum_hasher
        self.checksum_key = checksum_key
        self._checksum_source = checksum_source
        self.checksum_value = None

    @property
    def algorithm(self):
        """Return the S3 checksum algorithm."""
        return checksum_algorithm_from_header(self.checksum_key)

    def chunk_update(self, chunk, eof, *args, **kwargs):
        """Update and finish the checksum as input chunks are consumed."""
        # Note that "chunk" is just whatever was read from the input; this
        # says nothing about whether the underlying stream uses aws-chunked.
        self._checksum_hasher.update(chunk)
        if self._expected_length is None and self._checksum_source is None:
            # A s3api FULL_OBJECT MPU can receive an UploadPart with plain HTTP
            # chunking, no Content-Length, and no client checksum (swift
            # supports HTTP chunked transfer w/o chunksum while aws doesn't),
            # then the part controller installs this wrapper to calculate the
            # checksum. Valid aws-chunked requests already have a decoded
            # length here. For this unknown-length input, wait for EOF to
            # finish calculation.
            if not eof:
                return chunk
            self._expected_length = self.bytes_received
        if self.bytes_received < self._expected_length:
            # Wrapped input is likely to have timed out before this clause is
            # reached with eof=True, but just in case...
            error = eof
        elif self.bytes_received == self._expected_length:
            if self._checksum_source is None:
                self.checksum_value = base64_str(
                    self._checksum_hasher.digest())
                error = False
            else:
                # Lazy fetch checksum value because it may have come in
                # trailers.
                b64digest = self._checksum_source.get(self.checksum_key)
                try:
                    expected_raw_checksum = validate_checksum_value(
                        self._checksum_hasher, b64digest)
                except ValueError:
                    # Header values were validated before reading the body,
                    # so an invalid value here must have come from a trailer.
                    raise S3InputChecksumTrailerInvalid(self.checksum_key)
                error = (
                    self._checksum_hasher.digest() != expected_raw_checksum)
                if not error:
                    self.checksum_value = b64digest
        else:
            # The underlying wsgi.Input stops reading at content-length so we
            # do not expect to reach this clause, but just in case...
            error = True

        if error:
            self.close()
            # Since we do not return the last chunk, the PUT never completes.
            raise S3InputChecksumMismatch(self._checksum_hasher.name.upper())
        return chunk
