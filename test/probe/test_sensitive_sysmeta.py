#!/usr/bin/python -u
# Copyright (c) 2026 OpenStack Foundation
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

from io import BytesIO
from unittest import mock
from uuid import uuid4

from swift.common import direct_client, registry
from swiftclient import client

from test.probe.common import ReplProbeTest


class TestSensitiveSysmeta(ReplProbeTest):

    def test_sensitive_sysmeta_storage_matches_encryption(self):
        container = 'container-%s' % uuid4()
        obj = 'object-%s' % uuid4()
        header = 'X-Object-Sysmeta-Test-Digest'
        value = 'sensitive-digest'
        body = b'object body'

        for attr, value_for_attr in (
                ('_sensitive_sysmeta_names', set()),
                ('_swift_admin_info', {})):
            patcher = mock.patch.object(registry, attr, value_for_attr)
            patcher.start()
            self.addCleanup(patcher.stop)
        registry.register_sensitive_sysmeta(header)

        client.put_container(
            self.url, self.token, container,
            headers={'X-Storage-Policy': self.policy.name})
        internal_client = self.make_internal_client()
        internal_client.upload_object(
            BytesIO(body), self.account, container, obj,
            headers={header: value})
        encryption_enabled = bool(
            registry.get_swift_info(admin=True).get('admin', {})
            .get('encryption', {}).get('enabled', False))

        _status, response_headers, app_iter = internal_client.get_object(
            self.account, container, obj)
        self.assertEqual(body, b''.join(app_iter))
        self.assertEqual(value, response_headers[header])

        part, nodes = self.object_ring.get_nodes(
            self.account, container, obj)
        is_encrypted = []
        encrypted_header = 'X-Object-Sysmeta-Crypto-Sysmeta-Test-Digest'
        has_crypto_meta = []
        crypto_meta_header = 'X-Object-Sysmeta-Crypto-Meta'
        for node in nodes:
            headers, stored_body = direct_client.direct_get_object(
                node, part, self.account, container, obj,
                headers={'X-Backend-Storage-Policy-Index':
                         str(int(self.policy))})

            encrypted_value = headers.get(encrypted_header)
            clear_value = headers.get(header)
            self.assertNotEqual(bool(encrypted_value), bool(clear_value))
            is_encrypted.append(bool(encrypted_value))
            crypto_meta = headers.get(crypto_meta_header)
            has_crypto_meta.append(bool(crypto_meta))
            if encrypted_value:
                self.assertIsNone(clear_value)
                self.assertNotEqual(body, stored_body)
                self.assertNotIn(value, encrypted_value)
            else:
                self.assertIsNone(encrypted_value)
                self.assertEqual(body, stored_body)
                self.assertEqual(value, clear_value)

        self.assertTrue(all(is_encrypted) or not any(is_encrypted))
        self.assertEqual(is_encrypted, has_crypto_meta)
        self.assertEqual(encryption_enabled, all(is_encrypted))

        metadata = internal_client.get_object_metadata(
            self.account, container, obj)
        self.assertEqual(value, metadata.get(header.lower()), metadata)
