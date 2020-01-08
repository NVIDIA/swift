#!/usr/bin/python -u
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

from unittest import main

from swiftclient import client

from test.probe.common import ReplProbeTest


class TestObjectVersioning(ReplProbeTest):

    def _assert_account_level(self, container_name, hdr_cont_count,
                              hdr_obj_count, hdr_bytes, cont_count,
                              cont_bytes):

        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(hdr_cont_count, headers['x-account-container-count'])
        self.assertEqual(hdr_obj_count, headers['x-account-object-count'])
        self.assertEqual(hdr_bytes, headers['x-account-bytes-used'])
        self.assertEqual(len(containers), 1)
        container = containers[0]
        self.assertEqual(container_name, container['name'])
        self.assertEqual(cont_count, container['count'])
        self.assertEqual(cont_bytes, container['bytes'])

    def test_account_listing(self):
        versions_header_key = 'X-Versions-Enabled'

        # Create container1 and container2
        container_name = 'container1'
        obj_name = 'object1'
        client.put_container(self.url, self.token, container_name)

        # Assert account level sees it
        self._assert_account_level(
            container_name,
            hdr_cont_count='1',
            hdr_obj_count='0',
            hdr_bytes='0',
            cont_count=0,
            cont_bytes=0)

        # Enable versioning
        hdrs = {versions_header_key: 'True'}
        client.post_container(self.url, self.token, container_name, hdrs)

        # write multiple versions of same obj
        client.put_object(self.url, self.token, container_name, obj_name,
                          'version1')
        client.put_object(self.url, self.token, container_name, obj_name,
                          'version2')

        # Assert account level doesn't see object data yet, but it
        # does see the update for the hidden container
        self._assert_account_level(
            container_name,
            hdr_cont_count='2',
            hdr_obj_count='0',
            hdr_bytes='0',
            cont_count=0,
            cont_bytes=0)

        # Get to final state
        self.get_to_final_state()

        # Assert account level now sees updated values
        # N.B: Note difference in values between header and container listing
        # header object count is counting both symlink + object versions
        # listing count is counting only symlink (in primary container)
        self._assert_account_level(
            container_name,
            hdr_cont_count='2',
            hdr_obj_count='3',
            hdr_bytes='16',
            cont_count=1,
            cont_bytes=16)

        # directly delete primary container to leave an orphan hidden
        # container
        client.delete_object(self.url, self.token, container_name, obj_name)
        self.direct_delete_container(container=container_name)

        # Get to final state
        self.get_to_final_state()

        # the container count decreases, but object count and bytes don't
        # since orphan hidden container is still around
        self._assert_account_level(
            container_name,
            hdr_cont_count='1',
            hdr_obj_count='3',
            hdr_bytes='16',
            cont_count=3,
            cont_bytes=16)


if __name__ == '__main__':
    main()
