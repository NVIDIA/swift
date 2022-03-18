# Copyright (c) 2022 NVIDIA
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

import collections
import contextlib
import gzip
import hashlib
import json
import logging
import os
import six
import struct
import tempfile
import zlib

from swift.common.ring.utils import BYTES_TO_TYPE_CODE, network_order_array, \
    read_network_order_array

ZLIB_FLUSH_MARKER = b"\x00\x00\xff\xff"
# we could pull from io.DEFAULT_BUFFER_SIZE, but... 8k seems small
DEFAULT_BUFFER_SIZE = 2 ** 16


class ZlibReader(object):
    chunk_size = DEFAULT_BUFFER_SIZE

    def __init__(self, fp):
        self.fp = fp
        self.reset_decompressor()

    def reset_decompressor(self):
        self.pos = self.fp.tell()
        if self.pos == 0:
            # Has gzip header
            wbits = 16 + zlib.MAX_WBITS
        else:
            # Raw stream
            wbits = -zlib.MAX_WBITS
        self.decompressor = zlib.decompressobj(wbits)
        self.buffer = self.compressed_buffer = b""

    def seek(self, pos, whence=os.SEEK_SET):
        """
        Seek to the given point in the compressed stream.

        Buffers are dropped and a new decompressor is created (unless using
        ``os.SEEK_SET`` and the reader is already at the desired position).
        As a result, callers should be careful to ``seek()`` to flush
        boundaries, to ensure that subsequent ``read()`` calls work properly.

        Note that when using ``RingWriter``, all ``tell()`` results will be
        flush boundaries and appropriate to later use as ``seek()`` arguments.
        """
        if (pos, whence) == (self.pos, os.SEEK_SET):
            # small optimization for linear reads
            return
        self.fp.seek(pos, whence)
        self.reset_decompressor()

    def tell(self):
        return self.fp.tell()

    def _decompress_from_buffer(self, offset):
        if offset < 0:
            raise ValueError('buffer offset must be non-negative')
        chunk = self.compressed_buffer[:offset]
        self.compressed_buffer = self.compressed_buffer[offset:]
        self.pos += len(chunk)
        self.buffer += self.decompressor.decompress(chunk)

    def _buffer_chunk(self):
        """
        Buffer some data.

        The underlying file-like may or may not be read, though ``pos`` should
        always advance (unless we're already at EOF).

        Callers (i.e., ``read`` and ``readline``) should call this in a loop
        and monitor the size of ``buffer`` and whether we've hit EOF.

        :returns: True if we hit the end of the file, False otherwise
        """
        # stop at flushes, so we can save buffers on seek during a linear read
        x = self.compressed_buffer.find(ZLIB_FLUSH_MARKER)
        if x >= 0:
            self._decompress_from_buffer(x + len(ZLIB_FLUSH_MARKER))
            return False

        chunk = self.fp.read(self.chunk_size)
        if not chunk:
            self._decompress_from_buffer(len(self.compressed_buffer))
            return True
        self.compressed_buffer += chunk

        # if we found a flush marker in the new chunk, only go that far
        x = self.compressed_buffer.find(ZLIB_FLUSH_MARKER)
        if x >= 0:
            self._decompress_from_buffer(x + len(ZLIB_FLUSH_MARKER))
            return False

        # we may have *almost* found the flush marker;
        # gotta keep some of the tail
        keep = len(ZLIB_FLUSH_MARKER) - 1
        # note that there's no guarantee that buffer will actually grow --
        # but we don't want to have more in compressed_buffer than strictly
        # necessary
        self._decompress_from_buffer(len(self.compressed_buffer) - keep)
        return False

    def read(self, amount=-1):
        """
        Read ``amount`` uncompressed bytes.

        :raises IOError: if you try to read everything
        :raises zlib.error: if ``seek()`` was last called with a position
                            not at a flush boundary
        """
        if amount < 0:
            raise IOError("don't be greedy")

        while amount > len(self.buffer):
            if self._buffer_chunk():
                break

        data, self.buffer = self.buffer[:amount], self.buffer[amount:]
        return data

    def readline(self):
        # apparently pickle needs this?
        while b'\n' not in self.buffer:
            if self._buffer_chunk():
                break

        line, sep, self.buffer = self.buffer.partition(b'\n')
        return line + sep


class SectionReader(object):
    """
    A file-like wrapper that limits how many bytes may be read.

    Optionally, also verify data integrity.

    :param fp: a file-like object opened with mode "rb"
    :param length: the maximum number of bytes that should be read
    :param digest: optional hex digest of the expected bytes
    :param checksum: checksumming instance to be fed bytes and later compare
                     against ``digest``, e.g. ``hashlib.sha256()``
    """
    def __init__(self, fp, length, digest=None, checksum=None):
        self._fp = fp
        self._remaining = length
        self._digest = digest
        self._checksum = checksum

    def read(self, amt=None):
        """
        Read ``amt`` bytes, defaulting to "all remaining available bytes".
        """
        if amt is None or amt < 0:
            amt = self._remaining
        amt = min(amt, self._remaining)
        data = self._fp.read(amt)
        self._remaining -= len(data)
        if self._checksum:
            self._checksum.update(data)
        return data

    def read_ring_table(self, itemsize, partition_count):
        max_row_len = itemsize * partition_count
        type_code = BYTES_TO_TYPE_CODE[itemsize]
        return [
            read_network_order_array(type_code, row)
            for row in iter(lambda: self.read(max_row_len), b'')
        ]

    def close(self):
        """
        Verify that all bytes were read.

        If a digest was provided, also verify that the bytes read match
        the digest. Does *not* close the underlying file-like.

        :raises ValueError: if verification fails
        """
        if self._remaining:
            raise ValueError('Incomplete read; expected %d more bytes '
                             'to be read' % self._remaining)
        if self._digest and self._checksum.hexdigest() != self._digest:
            raise ValueError('Hash mismatch in block: %r found; %r expected' %
                             (self._checksum.hexdigest(), self._digest))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class IndexEntry(collections.namedtuple('IndexEntry', [
    'compressed_start',
    'uncompressed_start',
    'compressed_end',
    'uncompressed_end',
    'checksum_method',
    'checksum_value',
])):
    @property
    def uncompressed_length(self):
        if self.uncompressed_end is None:
            return None
        return self.uncompressed_end - self.uncompressed_start


class RingReader(object):
    """
    Helper for reading ring files.

    Provides format-version detection, and loads the index for v2 rings.
    """
    chunk_size = DEFAULT_BUFFER_SIZE

    def __init__(self, filename):
        self.raw_fp = open(filename, 'rb')
        self.zlib_fp = ZlibReader(self.raw_fp)
        self.index = {}

        magic = self.zlib_fp.read(4)
        if magic != b"R1NG":
            self.version = 0
        else:
            self.version, = struct.unpack("!H", self.zlib_fp.read(2))
            if self.version not in (1, 2):
                raise ValueError("Unsupported ring version: %d" % self.version)

        # get some size info from the end of the gzip
        self.raw_fp.seek(-4, os.SEEK_END)
        self.raw_size, = struct.unpack("<L", self.raw_fp.read(4))
        self.size = self.raw_fp.tell()

        self.load_index()

        self.zlib_fp.seek(0)

    def close(self):
        self.raw_fp.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def read(self, amt):
        """
        Read ``amt`` decompressed bytes from the zlib stream.
        """
        return self.zlib_fp.read(amt)

    def readline(self):
        # apparently pickle needs this?
        return self.zlib_fp.readline()

    def readinto(self, buffer):
        # Retained so pickle is happy on python 3.8.0 and 3.8.1 (i.e., versions
        # released with https://github.com/python/cpython/commit/91f4380c but
        # neither https://github.com/python/cpython/commit/9f37872e nor
        # https://github.com/python/cpython/commit/b19f7ecf )
        chunk = self.read(len(buffer))
        buffer[:len(chunk)] = chunk
        return len(chunk)

    def seek(self, offset, whence=os.SEEK_SET):
        return self.zlib_fp.seek(offset, whence)

    def tell(self):
        return self.zlib_fp.tell()

    def load_index(self):
        """
        If this is a v2 ring, load the index stored at the end.

        This will be done as part of initialization; users shouldn't need to
        do this themselves.
        """
        if self.version != 2:
            return

        # See notes in RingWriter.write_index and RingWriter.__exit__ for
        # where this 31 (= 18 + 13) came from.
        self.zlib_fp.seek(-31, os.SEEK_END)
        try:
            index_start, = struct.unpack("!Q", self.zlib_fp.read(8))
        except zlib.error:
            # TODO: we can still fix this if we're willing to read everything
            raise IOError("Could not read index offset "
                          "(was the file recompressed?)")
        self.zlib_fp.seek(index_start)
        # ensure index entries are sorted by position
        self.index = collections.OrderedDict(sorted(
            ((section, IndexEntry(*entry))
             for section, entry in json.loads(self.read_blob()).items()),
            key=lambda x: x[1]))

    def __contains__(self, section):
        if self.version != 2:
            return False
        return section in self.index

    def read_blob(self, fmt="!Q"):
        """
        Read a length-value encoded BLOB

        :param fmt: the format code used to write the length of the BLOB.
                    All v2 BLOBs use ``!Q``, but v1 may require ``!I``
        :returns: the BLOB value
        """
        prefix = self.zlib_fp.read(struct.calcsize(fmt))
        blob_length, = struct.unpack(fmt, prefix)
        return self.zlib_fp.read(blob_length)

    def read_section(self, section):
        """
        Read an entire section's data
        """
        with self.open_section(section) as reader:
            return reader.read()

    @contextlib.contextmanager
    def open_section(self, section):
        """
        Open up a section without buffering the whole thing in memory

        :raises ValueError: if there is no index
        :raises KeyError: if ``section`` is not in the index
        :raises IOError: if there is a conflict between the section size in
                         the index and the length at the start of the blob

        :returns: a ``SectionReader`` wrapping the section
        """
        if not self.index:
            raise ValueError("No index loaded")
        entry = self.index[section]
        self.zlib_fp.seek(entry.compressed_start)

        prefix = self.zlib_fp.read(8)
        blob_length, = struct.unpack("!Q", prefix)
        if entry.compressed_end is not None and \
                blob_length + 8 != entry.uncompressed_length:
            raise IOError("Inconsistent section size")

        if entry.checksum_method in ('md5', 'sha1', 'sha256', 'sha512'):
            checksum = getattr(hashlib, entry.checksum_method)(prefix)
            checksum_value = entry.checksum_value
        else:
            logging.getLogger('swift.ring').warning(
                "Ignoring unsupported checksum %s:%s for section %s",
                entry.checksum_method, entry.checksum_value, section)
            checksum = checksum_value = None

        with SectionReader(
            self.zlib_fp,
            blob_length,
            digest=checksum_value,
            checksum=checksum,
        ) as reader:
            yield reader


class RingWriter(object):
    """
    Helper for writing ring files to later be read by a ``RingReader``

    This has a few key features on top of a standard ``GzipFile``:

    * Atomic writes using a temporary file
    * Helpers for writing length-value encoded BLOBs
    * The ability to define named sections which will be written as
      an index at the end of the file
    * Flushes always use Z_FULL_FLUSH to support seeking.

    Note that the index will only be written if named sections were defined.
    """
    checksum_method = 'sha256'

    def __init__(self, filename, mtime=1300507380.0):
        self.filename = filename
        self.raw_fp = tempfile.NamedTemporaryFile(
            dir=os.path.dirname(filename),
            prefix=os.path.basename(filename),
            delete=False)
        self.gzip_fp = gzip.GzipFile(
            filename, mode='wb', fileobj=self.raw_fp, mtime=mtime)
        # index entries look like
        #   section: [
        #     compressed start,
        #     uncompressed start,
        #     compressed end,
        #     uncompressed end,
        #     checksum_method,
        #     checksum_value
        #   ]
        self.index = {}
        self.flushed = True
        self.current_section = None
        self.checksum = None
        self.pos = 0

    def __enter__(self):
        return self

    def __exit__(self, e, v, t):
        if e is not None:
            # on error, close it all out and unlink
            self.gzip_fp.close()
            self.raw_fp.close()
            os.unlink(self.raw_fp.name)
            return

        if self.index:
            # only write index if we made use of any sections
            self.write_index()

        # This does three things:
        #   * Flush the underlying compressobj (with Z_FINISH) and write
        #     the result
        #   * Write the (4-byte) CRC
        #   * Write the (4-byte) uncompressed length
        # NB: if we wrote an index, the flush writes exactly 5 bytes,
        # for 13 bytes total
        self.gzip_fp.close()

        self.raw_fp.flush()
        os.fsync(self.raw_fp.fileno())
        self.raw_fp.close()

        os.chmod(self.raw_fp.name, 0o644)
        os.rename(self.raw_fp.name, self.filename)

    def write(self, data):
        if not data:
            return 0
        self.flushed = False
        if self.checksum:
            self.checksum.update(data)
        self.pos += len(data)
        return self.gzip_fp.write(data)

    def flush(self):
        """
        Ensure the gzip stream has been flushed using Z_FULL_FLUSH.

        By default, the gzip module uses Z_SYNC_FLUSH; this ensures that all
        data is compressed and written to the stream, but retains some state
        in the compressor. A full flush, by contrast, ensures no state may
        carry over, allowing a reader to seek to the end of the flush and
        start reading with a fresh decompressor.
        """
        if not self.flushed:
            # always use full flushes; this allows us to just start reading
            # at the start of any section
            self.gzip_fp.flush(zlib.Z_FULL_FLUSH)
            self.flushed = True

    def tell(self):
        """
        Return the position in the underlying (compressed) stream.

        Since this is primarily useful to get a position you may seek to later
        and start reading, flush the writer first.

        If you want the position within the *uncompressed* stream, use the
        ``pos`` attribute.
        """
        self.flush()
        return self.raw_fp.tell()

    def set_compression_level(self, lvl):
        # two valid deflate streams may be concentated to produce another
        # valid deflate stream, so finish the one stream...
        self.flush()
        # ... so we can start up another with whatever level we want
        self.gzip_fp.compress = zlib.compressobj(
            lvl, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)

    @contextlib.contextmanager
    def section(self, name):
        """
        Define a named section.

        Return a context manager; the section contains whatever data is written
        within that context.

        The index will be updated to include the section and its starting
        positions upon entering the context; upon exiting normally, the index
        will be updated again with the ending positions and checksum
        information.
        """
        if self.current_section:
            raise ValueError('Cannot create new section; currently writing %r'
                             % self.current_section)
        if '*' in name:
            raise ValueError('Cannot write section with wildcard: %s' % name)
        if name in self.index:
            raise ValueError('Cannot write duplicate section: %s' % name)
        self.flush()
        self.current_section = name
        self.index[name] = IndexEntry(
            self.tell(), self.pos, None, None, None, None)
        self.checksum = getattr(hashlib, self.checksum_method)()
        try:
            yield self
            self.flush()
            self.index[name] = IndexEntry(
                self.index[name].compressed_start,
                self.index[name].uncompressed_start,
                self.tell(), self.pos,
                self.checksum_method, self.checksum.hexdigest())
        finally:
            self.flush()
            self.checksum = None
            self.current_section = None

    def write_magic(self, version):
        """
        Write our file magic for identifying post-pickle rings.

        :param version: the ring version; should be 1 or 2
        """
        if self.pos != 0:
            raise IOError("Magic must be written at the start of the file")
        self.write(struct.pack("!4sH", b"R1NG", version))

    def write_size(self, size, fmt="!Q"):
        """
        Write a BLOB-length.

        :param data: the size to write
        :param fmt: the struct format to use when writing the length.
                    All v2 BLOBs should use ``!Q``.
        """
        self.write(struct.pack(fmt, size))

    def write_blob(self, data, fmt="!Q"):
        """
        Write a length-value encoded BLOB.

        :param data: the bytes to write
        :param fmt: the struct format to use when writing the length.
                    All v2 BLOBs should use ``!Q``.
        """
        self.write_size(len(data), fmt)
        self.write(data)

    def write_json(self, data, fmt="!Q"):
        """
        Write a length-value encoded JSON BLOB.

        :param data: the JSON-serializable data to write
        :param fmt: the struct format to use when writing the length.
                    All v2 BLOBs should use ``!Q``.
        """
        json_data = json.dumps(data, sort_keys=True, ensure_ascii=True)
        self.write_blob(json_data.encode('ascii'), fmt)

    def write_ring_table(self, table):
        """
        Write a length-value encoded replica2part2dev table, or similar.
        Should *not* be used for v1 rings, as there's always a ``!Q`` size
        prefix, and values are written in network order.
        :param table: list of arrays
        """
        dev_id_bytes = table[0].itemsize if table else 0
        assignments = sum(len(a) for a in table)
        self.write_size(assignments * dev_id_bytes)
        for row in table:
            with network_order_array(row):
                if six.PY2:
                    # Can't just use tofile() because a GzipFile
                    # apparently doesn't count as an 'open file'
                    self.write(row.tostring())
                else:
                    row.tofile(self)

    def write_index(self):
        """
        Write the index and its starting position at the end of the file.

        Callers should not need to use this themselves; it will be done
        automatically when using the writer as a context manager.
        """
        with self.section('swift/index'):
            self.write_json(self.index)
        # switch to uncompressed
        self.set_compression_level(0)
        # ... which allows us to know that this will be exactly 18 bytes
        self.write_size(self.index['swift/index'][0])
        self.flush()
