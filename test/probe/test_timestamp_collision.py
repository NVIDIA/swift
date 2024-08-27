# Copyright (c) 2010-2025 OpenStack Foundation
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

from collections import defaultdict
from io import BytesIO
from unittest import mock
import os
import random

from swift.common import internal_client
from swift.common.utils import Timestamp, hash_path
from swift.obj.diskfile import _read_file_metadata

from test.probe.common import ECProbeTest

from swift.proxy.controllers.obj import MIMEPutter, ECObjectController
from eventlet import spawn, event, Timeout, sleep


class FragZipper(object):
    """
    Coordinates concurrent EC fragment writes to generate race conditions.

    This class implements a "zipper" pattern that synchronizes two concurrent
    upload requests writing EC fragments with the same timestamp. It ensures
    fragments are written in an interleaved fashion to simulate realistic
    race conditions.

    The zipper tracks which fragments each request has sent and uses win/lose
    sets to determine when one request should pause to let the other catch up,
    maintaining balanced progress. It only supports exactly two concurrent
    requests.
    """

    def __init__(self):
        # Mapping the id of the request to an integer, 0 is the first request
        # and 1 is the second request.
        self.tracking_ids = {}
        # Two sets tracking which fragment indexes each request has sent.
        self.sent = [set(), set()]
        # Two sets tracking fragments where this request "lost"
        self.lose = [set(), set()]
        # Two sets tracking fragments where this request "won"
        self.win = [set(), set()]
        self.waiting = None

    def track(self, req):
        """
        Track the request and return its ID.

        :param req: the request to the same EC object.
        :returns: 0 for the first request, 1 for the second request
        """
        return self.tracking_ids.setdefault(id(req), len(self.tracking_ids))

    def wait(self, req_id, fi):
        """
        Coordinate fragment write timing between two concurrent requests.

        :param req_id: the ID of the request from track() method.
        :param fi: the fragment index to coordinate.
        """
        my_id, other_id = req_id, (req_id + 1) % 2
        self.sent[my_id].add(fi)

        if fi in self.sent[other_id]:
            # request that sent this fragment wins this fragment
            self.win[my_id].add(fi)
            self.lose[other_id].add(fi)
        else:
            self.lose[my_id].add(fi)
            self.win[other_id].add(fi)

        # check if request should wait
        if (len(self.win[my_id]) > len(self.win[other_id]) or
                len(self.lose[my_id]) > len(self.lose[other_id])):
            if not self.waiting:  # prevent both requests from waiting
                e = self.waiting = event.Event()
                print(my_id, 'waiting', self.sent)
                e.wait()
        elif self.waiting:
            e, self.waiting = self.waiting, None
            e.send()
            print(my_id, 'wakeup', self.sent)
        else:
            print(my_id, 'proceeding', self.sent)
        print(my_id, 'status', fi, self.sent, self.lose, self.win)


class TestECCollision(ECProbeTest):

    def setUp(self):
        super(TestECCollision, self).setUp()
        # XXX I doubt there's a *good* reason _make_name returns bytes
        self.container_name = self.container_name.decode('utf8')
        self.object_name = self.object_name.decode('utf8')
        # XXX better to use random names each test; hit different parts
        self.container_name = 'badtest'
        self.object_name = 'badnews'
        self.swift = internal_client.InternalClient(
            '/etc/swift/internal-client.conf', 'probe-test', 1)
        self.swift.create_container(
            self.account, self.container_name,
            headers={'x-storage-policy': self.policy.name})

    def map_data_file_to_node(self):
        hpath = hash_path(self.account, self.container_name, self.object_name)
        p, nodes = self.policy.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        file_to_node = {}
        for node in nodes:
            part_dir = self.storage_dir(node, part=p)
            data_dir = os.path.join(part_dir, hpath[-3:], hpath)
            try:
                files = os.listdir(data_dir)
            except (OSError, IOError):
                continue
            for f in files:
                if not f.endswith('.data'):
                    continue
                path = os.path.join(data_dir, f)
                file_to_node[path] = node
        return file_to_node

    def do_upload(self, contents, headers):
        self.swift.upload_object(BytesIO(contents), self.account,
                                 self.container_name, self.object_name,
                                 headers=headers)

    def _meta_keys(self, key):
        key = key.title()
        return [
            'X-Object-Meta-%s' % key,
            'X-Object-Transient-Sysmeta-Crypto-Meta-%s' % key,
        ]

    def _collect_datafile_metadata(self, data_files, keys=None):
        if keys is not None:
            meta_keys = sum((self._meta_keys(k) for k in keys), [])
        else:
            meta_keys = None
        metadatas = {}
        for f in data_files:
            metadata = _read_file_metadata(f)
            if meta_keys is not None:
                metadata = {k: metadata[k] for k in metadata if k in meta_keys}
            metadatas[f] = metadata
        return metadatas

    def _collect_durable_files(self, data_files):
        return [df for df in data_files
                if os.path.splitext(df)[0].endswith('#d')]

    def test_overlapping_commit(self):
        contents = b'asdfb'
        now = Timestamp.now()
        orig_headers = {
            'x-timestamp': now.internal,
        }
        ready = event.Event()
        proceed = event.Event()

        orig_send_commit_confirmation = MIMEPutter.send_commit_confirmation

        def patched_send_commit_confirmation(putter):
            if not ready.ready():
                ready.send(None)
                proceed.wait()
            return orig_send_commit_confirmation(putter)

        with mock.patch.object(MIMEPutter, 'send_commit_confirmation',
                               patched_send_commit_confirmation):
            headers = dict(orig_headers)
            headers['x-object-meta-foo'] = 'bar'
            gt = spawn(self.do_upload, contents, headers)
            # let the first thread run up to commit
            ready.wait()
            # we should have a bunch of non-durable
            orig_df2node = self.map_data_file_to_node()
            self.assertEqual(self.policy.ec_n_unique_fragments,
                             len(orig_df2node))
            # nothing is durable
            self.assertEqual(0, len(self._collect_durable_files(orig_df2node)))

            # all the original metadata
            metadatas = self._collect_datafile_metadata(orig_df2node)
            foo_keys = self._meta_keys('foo')
            for df, m in metadatas.items():
                self.assertTrue(any(key in m for key in foo_keys),
                                '%s did not have foo metadata: %s' % (df, m))
            # overwrite from another thread
            headers = dict(orig_headers)
            headers['x-object-meta-bar'] = 'baz'
            with self.assertRaises(internal_client.UnexpectedResponse) as ctx:
                self.do_upload(contents, headers)
            self.assertEqual(503, ctx.exception.resp.status_int)
            df2node = self.map_data_file_to_node()
            self.assertEqual(self.policy.ec_n_unique_fragments, len(df2node))
            # nothing is durable yet
            self.assertEqual(0, len(self._collect_durable_files(df2node)))
            # all the original metadata
            metadatas = self._collect_datafile_metadata(df2node)
            foo_keys = self._meta_keys('foo')
            for df, m in metadatas.items():
                self.assertTrue(any(key in m for key in foo_keys),
                                '%s did not have foo metadata: %s' % (df, m))
            # new metadata never linked
            bar_metadatas = self._collect_datafile_metadata(
                df2node, keys=['bar'])
            self.assertFalse(any(m for m in bar_metadatas.values()))
            # let the first one finish now
            proceed.send(None)
            gt.wait()
        new_df2node = self.map_data_file_to_node()
        # all durable
        self.assertEqual(self.policy.ec_n_unique_fragments,
                         len(self._collect_durable_files(new_df2node)))
        # all the original metadata
        counts = defaultdict(int)
        metadata = self._collect_datafile_metadata(new_df2node, ['foo', 'bar'])
        foo_keys = self._meta_keys('foo')
        bar_keys = self._meta_keys('bar')
        for df, m in metadata.items():
            if any(key in m for key in foo_keys):
                counts['foo'] += 1
                self.assertFalse(any(key in m for key in bar_keys))
            else:
                self.fail('%s did not have foo metadata: %s' % (df, m))
        self.assertEqual(counts, {'foo': 6})

    def test_overlap_data_write_streams(self):
        start_chr = ord('a')
        # assuming segments are 1MiB
        num_segments = 3
        contents = b''.join(
            chr(i).encode() * (2 ** 20)
            for i in range(start_chr, start_chr + num_segments)
        )[:-300000]  # last frag is empty, second to last is short
        now = Timestamp.now()
        orig_headers = {
            'x-timestamp': now.internal,
        }

        original_transfer_data = ECObjectController._transfer_data

        zipper = FragZipper()

        def patched_transfer_data(controller, req, policy, data_source,
                                  putters, nodes, min_conns, etag_hasher):
            # annotate the putters with tracking_id based off req
            tracking_id = zipper.track(req)
            for putter in putters:
                putter.__tracking_id = tracking_id
            return original_transfer_data(
                controller, req, policy, data_source, putters, nodes,
                min_conns, etag_hasher)

        orig_end_of_object_data = MIMEPutter.end_of_object_data

        def patched_end_of_object_data(putter, footer_metadata):
            fi = footer_metadata['X-Object-Sysmeta-Ec-Frag-Index']
            orig_end_of_object_data(putter, footer_metadata)
            sleep(0.3)  # let the obj server flush or link or something
            print(putter.__tracking_id, 'end of data', fi,
                  footer_metadata['X-Object-Sysmeta-Ec-Etag'])
            zipper.wait(putter.__tracking_id, fi)

        results = []

        def safe_upload(contents, headers):
            try:
                resp = self.do_upload(contents, headers)
            except internal_client.UnexpectedResponse as e:
                resp = e.resp
            results.append(resp)

        with mock.patch.object(ECObjectController, '_transfer_data',
                               patched_transfer_data), \
                mock.patch.object(MIMEPutter, 'end_of_object_data',
                                  patched_end_of_object_data):
            headers = dict(orig_headers)
            headers['x-object-meta-foo'] = 'bar'
            gt1 = spawn(safe_upload, contents, headers)

            headers = dict(orig_headers)
            headers['x-object-meta-bar'] = 'baz'
            gt2 = spawn(safe_upload, contents, headers)

            try:
                with Timeout(10.0):
                    gt1.wait()
                    gt2.wait()
            except Timeout:
                self.fail('probably deadlock because of bugs '
                          '(or huge contents?); check logs')

        df2node = self.map_data_file_to_node()
        self.assertEqual(self.policy.ec_n_unique_fragments, len(df2node))
        # nothing is durable
        self.assertEqual(0, len(self._collect_durable_files(df2node)))
        # both responses were errors
        self.assertEqual([503, 503], [r.status_int for r in results])

        # metadata is all mixed up!
        counts = defaultdict(int)
        metadata = self._collect_datafile_metadata(df2node, ['foo', 'bar'])
        foo_keys = self._meta_keys('foo')
        bar_keys = self._meta_keys('bar')
        for m in metadata.values():
            if any(key in m for key in foo_keys):
                counts['foo'] += 1
                self.assertFalse(any(key in m for key in bar_keys))
            else:
                self.assertTrue(any(key in m for key in bar_keys))
                counts['bar'] += 1
                self.assertFalse(any(key in m for key in foo_keys))
        # OMM I get all of these scenarios!?  I wonder if there's a way to make
        # this more stable?
        self.assertIn(counts, [
            {'foo': 3, 'bar': 3},
            {'foo': 4, 'bar': 2},
            {'foo': 2, 'bar': 4},
        ])

    def test_overlap_writes_to_handoffs(self):
        contents = b'a' * 97
        now = Timestamp.now()
        headers = {
            'x-timestamp': now.internal,
        }
        self.do_upload(contents, headers)
        # check data files on disk
        df2node = self.map_data_file_to_node()
        self.assertEqual(self.policy.object_ring.replica_count,
                         len(df2node))
        # save one file and delete the rest
        save_file = random.choice(list(df2node.keys()))
        save_device = self.device_dir(df2node[save_file])
        self.kill_drive(save_device)
        for f in df2node:
            if f == save_file:
                continue
            os.unlink(f)

        # re-upload with same timestamp (add some metadata)
        headers['x-object-meta-foo'] = 'bar'
        self.do_upload(contents, headers)
        self.revive_drive(save_device)

        # files on disk look exactly the same (!!)
        new_df2node = self.map_data_file_to_node()
        self.assertEqual(df2node, new_df2node)
        # but the save_file metadata persists!
        counts = defaultdict(int)
        metadata = self._collect_datafile_metadata(df2node, ['foo'])
        foo_keys = self._meta_keys('foo')
        for m in metadata.values():
            if any(key in m for key in foo_keys):
                counts['foo'] += 1
            else:
                # safe_file had no metadata
                self.assertEqual({}, m)
                counts[None] += 1
        self.assertEqual(counts, {'foo': 5, None: 1})
