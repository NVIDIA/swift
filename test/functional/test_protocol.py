#!/usr/bin/python

# Copyright (c) 2010-2012 OpenStack Foundation
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

import time
import unittest

from swift.common.utils import distribute_evenly

from test.functional import check_response, retry, SkipTest
import test.functional as tf


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestHttpProtocol(unittest.TestCase):
    existing_metadata = None

    @classmethod
    def get_meta(cls):
        def head(url, token, parsed, conn):
            conn.request('HEAD', parsed.path, '', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(head)
        resp.read()
        return dict((k, v) for k, v in resp.getheaders() if
                    k.lower().startswith('x-account-meta'))

    @classmethod
    def clear_meta(cls, remove_metadata_keys):
        def post(url, token, parsed, conn, hdr_keys):
            headers = {'X-Auth-Token': token}
            headers.update((k, '') for k in hdr_keys)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)

        buckets = (len(remove_metadata_keys) - 1) // 90 + 1
        for batch in distribute_evenly(remove_metadata_keys, buckets):
            resp = retry(post, batch)
            resp.read()

    @classmethod
    def set_meta(cls, metadata):
        def post(url, token, parsed, conn, meta_hdrs):
            headers = {'X-Auth-Token': token}
            headers.update(meta_hdrs)
            conn.request('POST', parsed.path, '', headers)
            return check_response(conn)

        if not metadata:
            return
        resp = retry(post, metadata)
        resp.read()

    @classmethod
    def setUpClass(cls):
        # remove and stash any existing account user metadata before tests
        cls.existing_metadata = cls.get_meta()
        cls.clear_meta(cls.existing_metadata.keys())

    @classmethod
    def tearDownClass(cls):
        # replace any stashed account user metadata
        cls.set_meta(cls.existing_metadata)

    def test_invalid_path_info(self):
        if tf.skip:
            raise SkipTest

        def get(url, token, parsed, conn):
            path = "/info asdf"
            conn.request('GET', path, '', {'X-Auth-Token': token})
            return check_response(conn)

        resp = retry(get)
        resp.read()
        dt = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())

        self.assertEqual(resp.status, 412)
        self.assertIsNotNone(resp.getheader('X-Trans-Id'))
        self.assertIsNotNone(resp.getheader('X-Openstack-Request-Id'))
        self.assertEqual(dt, resp.getheader('Date'))
        self.assertIn('tx', resp.getheader('X-Trans-Id'))
        self.assertIn('tx', resp.getheader('X-Openstack-Request-Id'))
