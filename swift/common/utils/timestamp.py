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

"""Timestamp-related functions for use with Swift."""

import datetime
import functools
import math
import time
import random


NORMAL_FORMAT = "%016.05f"
INTERNAL_FORMAT = NORMAL_FORMAT + '_%016x'
SHORT_FORMAT = NORMAL_FORMAT + '_%x'
HEX_PART_DIGITS = 16
MAX_HEX_PART = (16 ** HEX_PART_DIGITS) - 1
# Note: Previous versions of the Timestamp class allocated the entire 16 digit
# hex_part to offset. To accommodate Timestamps with jitter, the offset of a
# legacy Timestamps has now been restricted to a maximum of 0x1fffffffffffffff,
# based on the assumption that no existing Timestamp has a larger offset.
# Timestamp offsets are restricted to 6 hex digits
NUM_OFFSET_DIGITS = 6
PRECISION = 1e-5
# raw time has units of PRECISION
MAX_RAW_TIME = 999999999999999
# Setting this to True will cause the internal format to always display
# extended digits - even when the value is equivalent to the normalized form.
# This isn't ideal during an upgrade when some servers might not understand
# the new time format - but flipping it to True works great for testing.
FORCE_INTERNAL = False  # or True


def _coerce(value):
    """
    Coerce ``value`` to an instance of a subclass of BaseTimestamp instance.

    :param value: value to coerce
    :return: an instance of a subclass of BaseTimestamp
    :raise: ValueError if ``value`` is not an instance of BaseTimestamp, nor
        can be parsed to a subclass of BaseTimestamp.
    """
    if isinstance(value, BaseTimestamp):
        return value
    # force parse by converting to string
    if not isinstance(value, (str, bytes)):
        value = str(value)
    try:
        return NormalTimestamp(value, check_bounds=False)
    except ValueError:
        return Timestamp(value, check_bounds=False)


@functools.total_ordering
class BaseTimestamp:
    """
    Abstract base class for timestamps. Subclasses must implement the _create
    and _parse methods to return a float timestamp value.
    """
    def __init__(self, timestamp, delta=0, check_bounds=True, **kwargs):
        if isinstance(timestamp, (float, int)):
            float_timestamp = self._create(timestamp, **kwargs)
        else:
            if isinstance(timestamp, bytes):
                timestamp_str = timestamp.decode('ascii')
            elif isinstance(timestamp, BaseTimestamp):
                timestamp_str = timestamp.internal
            else:
                timestamp_str = str(timestamp)
            float_timestamp = self._parse(timestamp_str, **kwargs)
        self.raw = int(round(float_timestamp / PRECISION))
        # add delta
        if delta:
            self.raw = self.raw + delta
            if self.raw <= 0:
                raise ValueError(
                    'delta must be greater than %d' % (-1 * self.raw))
        self.timestamp = round(float(self.raw * PRECISION), 5)
        if check_bounds:
            self._check_bounds()

    def _create(self, timestamp, **kwargs):
        raise NotImplementedError

    def _parse(self, timestamp_str, **kwargs):
        raise NotImplementedError

    def _check_bounds(self):
        if self.timestamp < 0:
            raise ValueError('timestamp cannot be negative')
        if self.timestamp >= 10000000000:
            raise ValueError('timestamp too large')

    @classmethod
    def now(cls, delta=0):
        """
        Returns an instance of a Timestamp at the current time.
        """
        return cls(time.time(), delta=delta)

    @classmethod
    def zero(cls):
        """
        Returns an instance of the smallest possible Timestamp.
        """
        return cls(0.0)

    def __repr__(self):
        return self.normal

    # TODO: do we need to be so brittle on the base class?
    def __str__(self):
        raise TypeError('You must specify which string format is required')

    def __float__(self):
        return self.timestamp

    def __int__(self):
        return int(self.timestamp)

    def __bool__(self):
        return bool(self.timestamp)

    @property
    def normal(self):
        """
        The normalised string representation of the timestamp's float part.
        """
        return NORMAL_FORMAT % self.timestamp

    @property
    def internal(self):
        """
        The full string representation of the timestamp. Subclasses may
        override this property such that it differs from ``normal``.

        This is the canonical string representation of the timestamp, used to
        evaluate equality and ordering of timestamps.
        """
        # note: BaseTimestamp has the 'internal' property even though it is
        # identical to the 'normal' property so that all subclasses provide
        # the same interface to their string representation(s). A caller can
        # use ts.internal for any type of BaseTimestamp.
        return self.normal

    @property
    def isoformat(self):
        """
        Get an isoformat string representation of the 'normal' part of the
        Timestamp with microsecond precision and no trailing timezone, for
        example::

            1970-01-01T00:00:00.000000

        :return: an isoformat string
        """
        t = float(self.normal)
        # On Python 3, round manually using ROUND_HALF_EVEN rounding
        # method, to use the same rounding method than Python 2. Python 3
        # used a different rounding method, but Python 3.4.4 and 3.5.1 use
        # again ROUND_HALF_EVEN as Python 2.
        # See https://bugs.python.org/issue23517
        frac, t = math.modf(t)
        us = round(frac * 1e6)
        if us >= 1000000:
            t += 1
            us -= 1000000
        elif us < 0:
            t -= 1
            us += 1000000
        dt = datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
        dt = dt.replace(microsecond=us)

        isoformat = dt.isoformat()
        # need to drop tzinfo
        isoformat = isoformat[:isoformat.index('+')]
        # python isoformat() doesn't include msecs when zero
        if len(isoformat) < len("1970-01-01T00:00:00.000000"):
            isoformat += ".000000"
        return isoformat

    @classmethod
    def from_isoformat(cls, date_string):
        """
        Parse an isoformat string representation of time to a Timestamp object.

        :param date_string: a string formatted as per a Timestamp.isoformat
            property.
        :return: an instance of  this class.
        """
        start = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        delta = start - EPOCH
        # This calculation is based on Python 2.7's Modules/datetimemodule.c,
        # function delta_to_microseconds(), but written in Python.
        return cls(str(delta.total_seconds()))

    def ceil(self):
        """
        Return the 'normal' part of the timestamp rounded up to the nearest
        integer number of seconds.

        This value should be used whenever the second-precision Last-Modified
        time of a resource is required.

        :return: a float value with second precision.
        """
        return math.ceil(float(self))

    def __eq__(self, other):
        if other is None:
            return False
        try:
            other_ts = _coerce(other)
        except ValueError:
            return NotImplemented

        return self.internal == other_ts.internal

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        if other is None:
            return False
        try:
            other_ts = _coerce(other)
        except ValueError:
            return NotImplemented
        if other_ts.timestamp < 0:
            return False
        if other_ts.timestamp >= 10000000000:
            return True
        return self.internal < other_ts.internal

    def __hash__(self):
        return hash(self.internal)

    def __invert__(self):
        return self.__class__((999999999999999 - self.raw) * PRECISION)


class NormalTimestamp(BaseTimestamp):
    """
    A NormalTimestamp encapsulates a timestamp rounded to the nearest
    deca-microsecond.

    The normalized form of time looks like a float with a fixed width to ensure
    stable string sorting - normalized timestamps look like "1402464677.04188".
    """
    # This subclass extends BaseTimestamp to provide a constructor that parses
    # and validates a string timestamp. The subclass also allows tests to mock
    # NormalTimestamp without changing the BaseTimestamp superclass behavior
    # that is also inherited by Timestamp.
    def __init__(self, timestamp, delta=0, check_bounds=True):
        """
        Create a new NormalTimestamp.

        :param timestamp: time in seconds since the Epoch, may be any of:

            * a float or integer
            * a string or bytes representation of a float that must not contain
              underscores
            * any other type that can be cast to a float including another
            * instance of BaseTimestamp

        :param delta: (int) deca-microsecond difference to be added to the
            ``timestamp`` value.
        :param check_bounds: if True (default) then a ValueError will be raised
            if the given timestamp is less than 0 or greater than the maximum
            time that can be represented by this class.
        """
        super().__init__(timestamp, delta, check_bounds=check_bounds)

    def _create(self, timestamp, **kwargs):
        return float(timestamp)

    def _parse(self, timestamp_str, **kwargs):
        if '_' in timestamp_str:
            # note: python will cast 1.2_3 to a float, and we do not want to
            # accidentally parse a Timestamp (with a hex_part) as a
            # NormalTimestamp
            raise ValueError('timestamp must not contain "_"')
        return float(timestamp_str)


class Timestamp(BaseTimestamp):
    """
    A Timestamp encapsulates a representation of a timestamp that uniquely
    identifies resources in Swift. It is typically used when Swift adds an
    X-Timestamp header to client requests. The Timestamp class supports
    internalized and normalized formatting of timestamps and also comparison of
    timestamp values.

    The internalized form of a Timestamp is a float part, which is the number
    of seconds since the epoch rounded to deca-microsecond precision, followed
    by a 16 digit hex part, e.g.:

        1402464677.04188_212345abcd000000
        <  float secs  >_<   hex part   >

    The fixed width of the parts ensures stable sort order.

    To support overwrites of existing data without modifying the original
    timestamp but still maintain consistency, an internal offset vector is
    maintained in the hex part. A timestamp with an offset therefore compares
    and sorts greater than the same timestamp with smaller offset, but less
    than a timestamp with a greater float part and/or random part. The offset
    is used by internal services (e.g. the reconciler). Normal client
    operations will not create a timestamp with an offset.

    Modern version 2 Timestamps encode the offset in the final 6 digits of the
    hex part. The first 10 digits of the hex part encode a randomly generated
    "jitter" component that differentiates timestamps within the same
    deca-microsecond. For example, a version 2 Timestamp with offset 1 has the
    following internalized form:

        1402464677.04188_212345abcd000001
        <  float secs  >_< jitter ><off.>

    The hex part is not exposed to clients in responses from Swift. Instead, a
    normalized form of the timestamps is used which comprises only the
    deca-microsecond precision float part, and is identical to the form of a
    NormalTimestamp, e.g.:

        1402464677.04188

    Legacy version 1 Timestamps allocated the entire 16 digit hex part to the
    offset. For example, a version 1 Timestamp with offset 1 has the following
    internalized form:

        1402464677.04188_0000000000000001
        <  float secs  >_<     offset   >

    When the offset of a version 1 Timestamp is 0 it is considered
    insignificant and the hex part is not included in the internalized form.
    When a version 1 Timestamp has a non-zero offset the hex part will always
    be represented in the internalized form, but is still excluded from the
    normalized form.

    Note: version 1 Timestamps are deprecated and supported for backwards
    compatibility only.

    Timestamps with an equivalent float part will compare and order by their
    hex part.  Timestamps with a greater float part will always compare and
    order greater than a Timestamp with a lesser float part regardless of its
    hex part.  String comparison and ordering is guaranteed for the
    internalized string format, and is backwards compatible for normalized
    timestamps which do not include a hex part.
    """

    def __init__(self, timestamp, offset=0, delta=0, check_bounds=True,
                 version=None):
        """
        Create a new Timestamp.

        Note: the ``timestamp.generate_timestamp`` function is the recommended
        way to create a fresh timestamp for a request.

        Note: the ``timestamp.Timestamp.zero`` method should be used to create
        a falsey timestamp that is at exactly zero time.

        Note: A sequence of version 2 Timestamps created *within the same
        deca-microsecond* is NOT guaranteed to be monotonically increasing
        because the jitter component of the hex part is randomly generated.

        :param timestamp: the value may be one of:
            * a float or integer: time in seconds since the Epoch; the
              Timestamp constructed will be in the deca-microsecond described
              by the value.
            * a string or bytes representation of a Timestamp
            * another instance of a Timestamp (the hex part is preserved when
              present).
            * any other type that can be cast to a string and parsed as a
              Timestamp, including another instance of BaseTimestamp.

        :param offset: (int) the internal offset vector. When ``timestamp`` is
            a float this value initialises the offset, otherwise this value
            will be added to any existing offset of the parsed ``timestamp``.
        :param delta: (int) deca-microsecond difference to be added to the
            ``timestamp`` value
        :param check_bounds: if True (default) then a ValueError will be raised
            if the given timestamp is less than 0 or greater than the maximum
            time that can be represented by this class.
        :param version: (int) the version of the timestamp to be generated.
            This argument is only allowed when ``timestamp`` is a float i.e.
            when generating a new timestamp. This should be set to 2 to
            generate a modern Timestamp with random jitter.
        """
        self.hex_part = 0
        super().__init__(timestamp, delta=delta, check_bounds=check_bounds,
                         version=version)
        self.increment_offset(offset)

    def _create(self, timestamp, version=1, **kwargs):
        version = version if version is not None else 1
        if version == 1:
            self.hex_part = 0
        elif version == 2:
            jitter_value = random.randint(0x000000000, 0xfffffffff)
            jitter_value += 0x2000000000
            # jitter is shifted to the left of the 6 offset digits
            self.hex_part = jitter_value << (4 * NUM_OFFSET_DIGITS)
        else:
            raise ValueError('Invalid version')
        return float(timestamp)

    def _parse(self, timestamp_str, version=None, **kwargs):
        if version is not None:
            raise TypeError('version not allowed when parsing timestamp')
        float_str, hex_str = timestamp_str.partition('_')[::2]
        if '_' in hex_str:
            raise ValueError('invalid literal for int() with base 16: '
                             '%r' % hex_str)
        if len(hex_str) > HEX_PART_DIGITS:
            raise ValueError('hex_part too long: %r' % hex_str)
        self.hex_part = int(hex_str, 16) if hex_str else 0
        return float(float_str)

    @property
    def _offset(self):
        if self.hex_part < 0x2000000000000000:
            # Previous versions of this class allowed offset to grow beyond the
            # current limit of 0xffffff so make some allowance for larger
            # values.
            return self.hex_part
        else:
            # The upper hex digits are used for jitter so offset is constrained
            # to the rightmost six digits.
            return self.hex_part & 0xffffff

    @_offset.setter
    def _offset(self, value):
        if not value:
            return
        if value < 0:
            raise ValueError('offset must be non-negative')

        if self.hex_part < 0x2000000000000000:
            max_offset = 0x1fffffffffffffff
        elif self.hex_part < 0x3000000000000000:
            max_offset = 0xffffff
        else:
            # we don't know how many hex digits have been allocated to offset
            # in a future version of this class so it is not safe to modify it
            raise ValueError('Cannot modify offset in an unrecognised hex '
                             'part encoding')

        if value > max_offset:
            raise ValueError('offset must be less than or equal to %d'
                             % max_offset)
        self.hex_part = (self.hex_part & ~max_offset) + value

    @property
    def offset(self):
        """
        This is here for backwards compatibility only. Callers should not
        attach any meaning to the absolute value of offset.
        """
        if self.hex_part >= 0x3000000000000000:
            # we don't know how many hex digits have been allocated to offset
            # in a future version of this class so it is not safe to return it
            raise AttributeError('Cannot access offset in an unrecognised '
                                 'hex_part encoding')
        return self._offset

    def increment_offset(self, value):
        """
        Increment the offset of the timestamp by the given value.

        :raises ValueError: if value is negative or if the resulting offset
            would exceed the maximum supported offset.
        """
        if not value:
            return
        if value < 0:
            raise ValueError('offset must be non-negative')
        self._offset += value
        return self._offset

    @classmethod
    def now(cls, offset=0, delta=0, version=1):
        """
        Returns an instance of a Timestamp at the current time.

        :param offset: (int) the second internal offset vector
        :param delta: (int) deca-microsecond difference to be added to the
            current time.
        :param version: (int) the version of the timestamp to be generated.
            This should be set to 2 to generate a modern version 2 Timestamp
            with random jitter.
        """
        return cls(time.time(), offset=offset, delta=delta, version=version)

    def __repr__(self):
        return self._internal_format(self.timestamp, self.hex_part, True)

    def __bool__(self):
        return super().__bool__() or bool(self.hex_part)

    def _internal_format(self, timestamp, hex_part, force_internal=False):
        if hex_part or force_internal:
            return INTERNAL_FORMAT % (timestamp, hex_part)
        else:
            return NORMAL_FORMAT % timestamp

    @property
    def internal(self):
        return self._internal_format(
            self.timestamp, self.hex_part, FORCE_INTERNAL)

    @property
    def short(self):
        # TODO:: For modern object PUT x-timestamps that always have a hex_part
        #  whose string representation will have 16 digits, this format is
        #  identical to the internal format. Furthermore, this format is only
        #  used to encode object data timestamps (i.e. PUT x-timestamps) in the
        #  encode_timestamps function. As such it has become redundant and
        #  should be removed/deprecated.
        if self.hex_part or FORCE_INTERNAL:
            return SHORT_FORMAT % (self.timestamp, self.hex_part)
        else:
            return self.normal

    def __invert__(self):
        inv_raw = (MAX_RAW_TIME - self.raw) * PRECISION
        if not self.hex_part:
            # legacy Timestamps did not allow inversion of hex_parts
            inv_hex_part = 0
        else:
            inv_hex_part = MAX_HEX_PART - self.hex_part
        return Timestamp(self._internal_format(inv_raw, inv_hex_part))

    def normalized(self):
        """
        Get a NormalTimestamp clone of this Timestamp without any hex
        extension.

        Normalized timestamps have less differentiation from each other than
        extended timestamps; only use this method if you understand the
        implication of that loss of differentiation.

        :returns: an instance of NormalTimestamp.
        """
        return NormalTimestamp(self.normal)


def timestamp2():
    """
    Generate a version 2 Timestamp using the current time and random jitter.

    :returns: an instance of Timestamp.
    """
    return Timestamp.now(version=2)


def encode_timestamps(t1, t2=None, t3=None, explicit=False):
    """
    Encode up to three timestamps into a string. Unlike a Timestamp object, the
    encoded string does NOT used fixed width fields and consequently no
    relative chronology of the timestamps can be inferred from lexicographic
    sorting of encoded timestamp strings.

    The format of the encoded string is:
        <t1>[<+/-><t2 - t1>[<+/-><t3 - t2>]]

    i.e. if t1 = t2 = t3 then just the string representation of t1 is returned,
    otherwise the time deltas for t2 and t3 are appended. If explicit is True
    then the deltas for t2 and t3 are always appended even if zero.

    Note: any hex_part value in t1 will be preserved, but hex_parts on t2 and
    t3 are not preserved. In the anticipated use cases for this method (and the
    inverse decode_timestamps method) the timestamps passed as t2 and t3 are
    not expected to have hex_parts as they will be timestamps associated with a
    POST request. In the case where the encoding is used in a container objects
    table row, t1 could be the PUT or DELETE time but t2 and t3 represent the
    content type and metadata times (if different from the data file) i.e.
    correspond to POST timestamps. In the case where the encoded form is used
    in a .meta file name, t1 and t2 both correspond to POST timestamps.
    """
    form = '{0}'
    values = [t1.short]
    if t2 is not None:
        # XXX note that any hex_part that t2 or t3 might have is NOT included
        # in the encoded combination
        t2_t1_delta = t2.raw - t1.raw
        explicit = explicit or (t2_t1_delta != 0)
        values.append(t2_t1_delta)
        if t3 is not None:
            t3_t2_delta = t3.raw - t2.raw
            explicit = explicit or (t3_t2_delta != 0)
            values.append(t3_t2_delta)
        if explicit:
            form += '{1:+x}'
            if t3 is not None:
                form += '{2:+x}'
    return form.format(*values)


def decode_timestamps(encoded, explicit=False):
    """
    Parses a string of the form generated by encode_timestamps and returns
    a tuple of the three component timestamps. If explicit is False, component
    timestamps that are not explicitly encoded will be assumed to have zero
    delta from the previous component and therefore take the value of the
    previous component. If explicit is True, component timestamps that are
    not explicitly encoded will be returned with value None.

    :return: a tuple(Timestamp, Timestamp, Timestamp)
    """
    # TODO: some tests, e.g. in test_replicator, put float timestamps values
    # into container db's, hence this defensive check, but in real world
    # this may never happen.
    if not isinstance(encoded, str):
        ts = Timestamp(encoded)
        return ts, ts, ts

    parts = []
    signs = []
    pos_parts = encoded.split('+')
    for part in pos_parts:
        # parse time components and their signs
        # e.g. x-y+z --> parts = [x, y, z] and signs = [+1, -1, +1]
        neg_parts = part.split('-')
        parts = parts + neg_parts
        signs = signs + [1] + [-1] * (len(neg_parts) - 1)
    t1 = Timestamp(parts[0])
    t2 = t3 = None
    if len(parts) > 1:
        t2 = t1
        delta = signs[1] * int(parts[1], 16)
        # if delta = 0 we want t2 = t3 = t1 in order to
        # preserve any hex_part in t1 - only construct a distinct
        # timestamp if there is a non-zero delta.
        if delta:
            # we lost the original hex_part of t2 when encoding, so we have no
            # choice other than to force it to zero when decoding
            t2 = Timestamp(str((t1.raw + delta) * PRECISION))
    elif not explicit:
        t2 = t1
    if len(parts) > 2:
        t3 = t2
        delta = signs[2] * int(parts[2], 16)
        if delta:
            # we lost the original hex_part of t2 when encoding, so we have no
            # choice other than to force it to zero when decoding
            t3 = Timestamp(str((t2.raw + delta) * PRECISION))
    elif not explicit:
        t3 = t2
    return t1, t2, t3


def normalize_timestamp(timestamp):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx.xxxxx (10.5) format.

    Note that timestamps using values greater than or equal to November 20th,
    2286 at 17:46 UTC will use 11 digits to represent the number of
    seconds.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    return Timestamp(timestamp).normal


EPOCH = datetime.datetime(1970, 1, 1)


def last_modified_date_to_timestamp(last_modified_date_str):
    """
    Convert a last modified date (like you'd get from a container listing,
    e.g. 2014-02-28T23:22:36.698390) to a float.
    """
    return Timestamp.from_isoformat(last_modified_date_str)


def normalize_delete_at_timestamp(timestamp, high_precision=False):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx (10) or xxxxxxxxxx.xxxxx (10.5) format.

    Note that timestamps less than 0000000000 are raised to
    0000000000 and values greater than November 20th, 2286 at
    17:46:39 UTC will be capped at that date and time, resulting in
    no return value exceeding 9999999999.99999 (or 9999999999 if
    using low-precision).

    This cap is because the expirer is already working through a
    sorted list of strings that were all a length of 10. Adding
    another digit would mess up the sort and cause the expirer to
    break from processing early. By 2286, this problem will need to
    be fixed, probably by creating an additional .expiring_objects
    account to work from with 11 (or more) digit container names.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    # Note: high_precision is used by container_deleter and SLO async_delete.
    # Deca-microsecond precision is considered sufficient for the
    # high_precision format; an object that was PUT in the same
    # deca-microsecond won't be deleted. Adding a hex_part extension to the
    # delete timestamp won't necessarily help, because the hex_part is not
    # guaranteed to advance monotonically within the same deca-microsecond, so
    # the delete timestamp could still sort 'older' than a previous PUT in the
    # same deca-microsecond.
    fmt = '%016.5f' if high_precision else '%010d'
    return fmt % min(max(0, float(timestamp)), 9999999999.99999)


def generate_timestamp(resource_type, request_method):
    """
    Returns a timestamp of the appropriate type for the given resource_type and
    request method.

    This is the preferred way to create an X-Timestamp value for a request.

    :param resource_type: one of 'account', 'container', 'object' or None.
    :param request_method: a request method.
    :returns: timestamp, an instance of BaseTimestamp.
    """
    # TODO: this comment is stale/wrong, but should it be true?
    # note: always use Timestamp.now() to generate the timestamp, and then cast
    # to a NormalTimestamp when necessary, so that tests can mock only
    # Timestamp.now() to mock timestamps for *all request types*.
    if (resource_type
            and resource_type.lower() == 'object'
            and request_method == 'PUT'):
        return timestamp2()
    else:
        return NormalTimestamp.now()


def parse_timestamp(value, resource_type, request_method):
    """
    Parse a timestamp string and validate it for the given resource_type and
    request_method.

    :param value: timestamp string.
    :param resource_type: one of 'account', 'container', 'object' or None.
    :param request_method: a request method.
    :returns: timestamp, an instance of BaseTimestamp.
    :raises: ValueError if the timestamp string is invalid for the given
        resource_type and request_method.
    """
    if not resource_type:
        return NormalTimestamp(value)
    elif resource_type.lower() == 'object' and request_method == 'PUT':
        return Timestamp(value)
    elif resource_type.lower() == 'object' and request_method == 'DELETE':
        # Extended timestamps are allowed on object DELETE to accommodate the
        # reconciler deleting misplaced objects.
        return Timestamp(value)
    elif request_method in ('HEAD', 'GET'):
        # Extended timestamps are allowed on HEADs and GETS to accommodate
        # subrequests that inherit an x-timestamp from an object PUT requests
        # e.g. a container GET for shard ranges.
        return Timestamp(value)
    else:
        return NormalTimestamp(value)
