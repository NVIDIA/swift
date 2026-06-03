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

import base64
import hashlib
from io import BytesIO
from unittest import mock
from uuid import uuid4

from swift.common import direct_client, registry
from swift.common.swob import normalize_etag
from swift.common.utils import md5
from swiftclient import client

from test.probe.common import ReplProbeTest
from test.s3api import get_s3_client


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

    def test_s3api_checksum_sysmeta_encryption(self):
        if not self.cluster_info.get('s3api', {}).get(
                'allow_store_checksums', False):
            self.skipTest('This test requires allow_store_checksums=true')
        bucket = self._make_name('bucket-').decode()
        obj = self._make_name('object-').decode()
        body = b'object body'

        s3 = get_s3_client()
        client.put_container(
            self.url, self.token, bucket,
            headers={'X-Storage-Policy': self.policy.name})
        s3.put_object(Bucket=bucket, Key=obj, Body=body,
                      ChecksumAlgorithm='SHA256')

        head_resp = s3.head_object(Bucket=bucket, Key=obj,
                                   ChecksumMode='ENABLED')
        etag = head_resp['ETag'].strip('"')
        checksum_value = head_resp['ChecksumSHA256']
        self.assertEqual(md5(body, usedforsecurity=False).hexdigest(), etag)
        self.assertEqual(
            base64.b64encode(hashlib.sha256(body).digest()).decode('ascii'),
            checksum_value)

        encrypted_etag_header = \
            'X-Object-Sysmeta-Crypto-Etag'
        checksum_header = 'X-Object-Sysmeta-S3Api-Checksum-Value'
        encrypted_checksum_header = \
            'X-Object-Sysmeta-Crypto-Sysmeta-S3Api-Checksum-Value'
        crypto_meta_header = 'X-Object-Sysmeta-Crypto-Meta'
        is_encrypted_etag = []
        is_encrypted_checksum = []
        has_crypto_meta = []
        part, nodes = self.object_ring.get_nodes(self.account, bucket, obj)
        for node in nodes:
            headers, stored_body = direct_client.direct_get_object(
                node, part, self.account, bucket, obj,
                headers={'X-Backend-Storage-Policy-Index':
                         str(int(self.policy))})
            encrypted_etag = headers.get(encrypted_etag_header)
            is_encrypted_etag.append(bool(encrypted_etag))
            encrypted_checksum = headers.get(encrypted_checksum_header)
            clear_value = headers.get(checksum_header)
            self.assertNotEqual(bool(encrypted_checksum), bool(clear_value))
            is_encrypted_checksum.append(bool(encrypted_checksum))
            crypto_meta = headers.get(crypto_meta_header)
            has_crypto_meta.append(bool(crypto_meta))
            if encrypted_checksum:
                # encryption is enabled: the etag and checksum are not stored
                # in the clear, and the stored values reveal nothing
                self.assertIsNone(clear_value)
                self.assertNotEqual(body, stored_body)
                self.assertNotIn(checksum_value, encrypted_checksum)
                self.assertNotIn(normalize_etag(etag), encrypted_etag)
                # etag should also be encrypted
                self.assertNotEqual(normalize_etag(etag),
                                    normalize_etag(headers['etag']))
            else:
                self.assertIsNone(encrypted_checksum)
                self.assertIsNone(encrypted_etag)
                self.assertEqual(body, stored_body)
                self.assertEqual(checksum_value, clear_value)
                self.assertEqual(normalize_etag(etag),
                                 normalize_etag(headers['etag']))

        self.assertTrue(all(is_encrypted_checksum)
                        or not any(is_encrypted_checksum))
        self.assertEqual(is_encrypted_checksum, has_crypto_meta)
        self.assertEqual(is_encrypted_checksum, is_encrypted_etag)
