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
import unittest
from collections import defaultdict
from io import BytesIO
from unittest import mock
import os
import random

from swift.common import internal_client
from swift.common.utils import Timestamp, hash_path, GreenAsyncPile
from swift.obj.diskfile import _read_file_metadata

from test.probe.common import ECProbeTest

from swift.proxy.controllers.obj import MIMEPutter, ECObjectController
from eventlet import spawn, event, Timeout, sleep


class FragZipper(object):
    """
    Coordinate two EC writes to control which request sends each fragment
    footer first.

    winners_per_req specifies how many fragment indexes each request sends
    before its peer. It must contain two positive counts whose sum is the
    number of fragment indexes.
    """

    # These are scheduling roles, not result roles. The trailing request starts
    # parked so the leading request sends first.
    leading_id = 1
    trailing_id = 0

    def __init__(self, winners_per_req, debug=False):
        self.tgt_win_per_req = winners_per_req
        self.debug = debug
        self.sent = [set(), set()]
        self.first_sender = {}
        self.waiting = [None, None]

    def wait_to_start(self, req_id):
        if req_id == self.trailing_id:
            self.wait(req_id)

    def log(self, req_id, msg):
        if self.debug:
            print('%s %d %s, wins=%s, sent=%s'
                  % ('\t' * req_id, req_id, msg,
                     [self.wins(0), self.wins(1)], self.sent))

    def wakeup(self, req_id):
        if self.waiting[req_id]:
            waiting = self.waiting[req_id]
            self.waiting[req_id] = None
            self.log(req_id, 'wakeup')
            waiting.send()

    def wait(self, req_id):
        self.waiting[req_id] = event.Event()
        self.log(req_id, 'wait')
        self.waiting[req_id].wait()

    def wins(self, req_id):
        return sum(1 for winner in self.first_sender.values()
                   if winner == req_id)

    def wins_remaining(self, req_id):
        return self.tgt_win_per_req[req_id] - self.wins(req_id)

    def after_send(self, req_id, frag_index):
        """
        Record a sent fragment footer and advance the other request as needed.
        """
        self.sent[req_id].add(frag_index)
        self.first_sender.setdefault(frag_index, req_id)
        self.log(req_id, 'sent %s' % frag_index)

        other_id = (req_id + 1) % 2
        while (self.wins_remaining(req_id) <= 0
               and (self.wins_remaining(other_id)
                    or len(self.sent[other_id]) < len(self.sent[req_id]))):
            # Once this request has its wins, let the peer take its wins and
            # then alternate the remaining losing fragment writes.
            self.wakeup(other_id)
            self.wait(req_id)
        if len(self.sent[req_id]) >= sum(self.tgt_win_per_req):
            self.wakeup(other_id)
        self.log(req_id, 'proceed')


class TestFragZipper(unittest.TestCase):
    def run_zipper(self, target_wins):
        zipper = FragZipper(target_wins, debug=True)

        def send_all_fragments(req_id):
            zipper.wait_to_start(req_id)
            for frag_index in range(sum(target_wins)):
                sleep(random.random() / 100)
                zipper.after_send(req_id, frag_index)
            return True

        pile = GreenAsyncPile(2)
        pile.spawn(send_all_fragments, 0)
        pile.spawn(send_all_fragments, 1)

        self.assertEqual([True, True], pile.waitall(1))
        self.assertEqual(
            sum(target_wins), len(zipper.first_sender),
            zipper.first_sender)
        return [zipper.wins(0), zipper.wins(1)]

    def test_balanced(self):
        self.assertEqual([3, 3], self.run_zipper([3, 3]))

    def test_biased_2_4(self):
        self.assertEqual([2, 4], self.run_zipper([2, 4]))

    def test_biased_1_5(self):
        self.assertEqual([1, 5], self.run_zipper([1, 5]))


class TestECCollision(ECProbeTest):

    def setUp(self):
        super(TestECCollision, self).setUp()
        # There doesn't seem to be a *good* reason _make_name returns bytes
        self.container_name = self.container_name.decode('utf8')
        self.object_name = self.object_name.decode('utf8')
        self.swift = internal_client.InternalClient(
            '/etc/swift/internal-client.conf', 'probe-test', 1)
        self.swift.create_container(
            self.account, self.container_name,
            headers={'x-storage-policy': self.policy.name})

    def map_data_files_to_nodes(self, part, nodes):
        hpath = hash_path(self.account, self.container_name, self.object_name)
        file_to_node = {}
        for node in nodes:
            part_dir = self.storage_dir(node, part=part)
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

    def map_data_files_to_primary_nodes(self):
        part, nodes = self.policy.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        return self.map_data_files_to_nodes(part, nodes)

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
        """
        Returns a dict mapping data file paths to each file's metadata.
        """
        if keys is not None:
            meta_keys = sum((self._meta_keys(k) for k in keys), [])
        else:
            meta_keys = None
        file_to_metadata = {}
        for f in data_files:
            metadata = _read_file_metadata(f)
            if meta_keys is not None:
                metadata = {k: metadata[k] for k in metadata if k in meta_keys}
            file_to_metadata[f] = metadata
        return file_to_metadata

    def _map_metadata_keys(self, file_to_metadata, keys):
        """
        Returns a dict mapping metadata keys to sets of data file paths in
        which the keys are found.
        """
        key_to_path = defaultdict(set)
        for key in keys:
            meta_keys = self._meta_keys(key)
            for path, metadata in file_to_metadata.items():
                if any(key in metadata for key in meta_keys):
                    key_to_path[key].add(path)
        return key_to_path

    def _primary_data_files_by_metadata(self, keys):
        data_files = self.map_data_files_to_primary_nodes()
        file_to_metadata = self._collect_datafile_metadata(data_files, keys)
        return data_files, self._map_metadata_keys(file_to_metadata, keys)

    def _collect_durable_files(self, data_files):
        return [df for df in data_files
                if os.path.splitext(df)[0].endswith('#d')]

    def _do_test_overlap_data_write_streams(self, winners):
        # sanity checks...
        self.assertEqual(self.policy.ec_n_unique_fragments, sum(winners))
        self.assertGreater(min(winners), 0)

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

        zipper = FragZipper(winners, debug=True)

        def patched_transfer_data(controller, req, policy, data_source,
                                  putters, nodes, min_conns, etag_hasher):
            req_id = int(req.headers['X-Fragzipper-Req-Id'])
            for putter in putters:
                putter._fragzipper_req_id = req_id
            zipper.wait_to_start(req_id)
            return original_transfer_data(
                controller, req, policy, data_source, putters, nodes,
                min_conns, etag_hasher)

        orig_end_of_object_data = MIMEPutter.end_of_object_data

        def patched_end_of_object_data(putter, footer_metadata):
            fi = footer_metadata['X-Object-Sysmeta-Ec-Frag-Index']
            req_id = putter._fragzipper_req_id
            orig_end_of_object_data(putter, footer_metadata)
            # The footer has been sent; give the object server a chance to
            # process that first-phase write before scheduling the next one.
            sleep(0.3)
            print(req_id, 'end of data', fi,
                  footer_metadata['X-Object-Sysmeta-Ec-Etag'])
            zipper.after_send(req_id, fi)

        results = []

        def safe_upload(req_id, contents, headers):
            headers = dict(headers)
            headers['X-Fragzipper-Req-Id'] = str(req_id)
            try:
                self.do_upload(contents, headers)
                status = 201  # yuk! internal client doesn't return good resp
            except internal_client.UnexpectedResponse as e:
                status = e.resp.status_int
            results.append(status)

        with mock.patch.object(ECObjectController, '_transfer_data',
                               patched_transfer_data), \
                mock.patch.object(MIMEPutter, 'end_of_object_data',
                                  patched_end_of_object_data):
            headers = dict(orig_headers)
            headers['x-object-meta-foo'] = 'bar'
            gt1 = spawn(safe_upload, 0, contents, headers)

            headers = dict(orig_headers)
            headers['x-object-meta-bar'] = 'baz'
            gt2 = spawn(safe_upload, 1, contents, headers)

            try:
                with Timeout(10.0):
                    gt1.wait()
                    gt2.wait()
            except Timeout:
                self.fail('probably deadlock because of bugs '
                          '(or huge contents?); check logs')
        return results

    def test_overlap_data_write_streams_no_durable_set(self):
        # each request succeeds in writing 3 frags, so neither reaches quorum,
        # no frags are made durable, and both requests return 503
        results = self._do_test_overlap_data_write_streams([3, 3])
        keys = ['foo', 'bar']
        df2node, key_to_files = self._primary_data_files_by_metadata(keys)
        self.assertEqual(self.policy.ec_n_unique_fragments, len(df2node))

        # no mixed fragment set is durable
        durable_files = set(self._collect_durable_files(df2node))
        actual = {
            'durable': len(durable_files),
            'durable_by_metadata': {
                key: len(files & durable_files)
                for key, files in key_to_files.items()},
        }
        self.assertEqual({
            'durable': 0,
            'durable_by_metadata': {'foo': 0, 'bar': 0},
        }, actual)
        # both responses were errors
        self.assertEqual([503, 503], results)
        # metadata is all mixed up!
        self.assertEqual(
            {'foo': 3, 'bar': 3},
            {key: len(files) for key, files in key_to_files.items()})
        self.assertFalse(
            key_to_files['foo'].intersection(key_to_files['bar']))

    def test_overlap_data_write_streams_one_durable_set(self):
        # the first request succeeds in writing 5 frags *then waits for the
        # second request to write its frags*;
        # the second request fails to write 5/6 frags and returns 503;
        # the first request makes its 5 frags durable and returns 201
        results = self._do_test_overlap_data_write_streams([5, 1])
        # one response is success
        self.assertEqual({201, 503}, set(results), results)

        keys = ['foo', 'bar']
        df2node, key_to_files = self._primary_data_files_by_metadata(keys)
        self.assertEqual(self.policy.ec_n_unique_fragments, len(df2node))

        # metadata is all mixed up!
        self.assertEqual(
            {'foo': 5, 'bar': 1},
            {key: len(files) for key, files in key_to_files.items()})
        # but mutually exclusive w.r.t. files
        self.assertFalse(
            key_to_files['foo'].intersection(key_to_files['bar']))

        # one set of 5 frags durable
        orig_durable_data_files = self._collect_durable_files(df2node)
        self.assertEqual(self.policy.ec_n_unique_fragments - 1,
                         len(orig_durable_data_files), orig_durable_data_files)
        durable_metadatas = self._collect_datafile_metadata(
            orig_durable_data_files)
        orig_key_to_files = self._map_metadata_keys(durable_metadatas, keys)
        # all durables have same metadata
        self.assertEqual(1, len(orig_key_to_files))
        orig_durable_key = list(orig_key_to_files.keys())[0]
        self.assertIn(orig_durable_key, keys, orig_key_to_files)
        self.assertEqual(self.policy.ec_n_unique_fragments - 1,
                         len(orig_key_to_files[orig_durable_key]),
                         orig_key_to_files)

        # now run the reconstructor
        self.reconstructor.once()

        # now we have 6 durables...
        df2node = self.map_data_files_to_primary_nodes()
        new_durable_data_files = self._collect_durable_files(df2node)
        self.assertEqual(self.policy.ec_n_unique_fragments,
                         len(new_durable_data_files), new_durable_data_files)
        durable_metadatas = self._collect_datafile_metadata(
            new_durable_data_files)
        new_key_to_files = self._map_metadata_keys(durable_metadatas, keys)
        # the set of 5 are unchanged
        self.assertEqual(orig_key_to_files[orig_durable_key],
                         new_key_to_files[orig_durable_key])
        # BUT there's also a durable with different metadata :(
        self.assertEqual(set(keys), set(new_key_to_files.keys()))
