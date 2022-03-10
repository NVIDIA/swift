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
import json
import unittest

from swift.common.middleware.s3api.controllers.inventory import \
    InventoryConfiguration
from swift.common.swob import Request, HTTPNoContent, HTTPNotFound
from swift.common.middleware.s3api.etree import fromstring, tostring, \
    DocumentInvalid
from test.unit import mock_timestamp_now
from test.unit.common.middleware.s3api import S3ApiTestCase

# conforms to schema but using unsupported fields and values
UNSUPPORTED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<InventoryConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Id>report1</Id>
   <IsEnabled>true</IsEnabled>
   <Filter>
      <Prefix>filterPrefix</Prefix>
   </Filter>
   <Destination>
      <S3BucketDestination>
         <Format>CSV</Format>
         <AccountId>123456789012</AccountId>
         <Bucket>arn:aws:s3:::destination-bucket</Bucket>
         <Prefix>prefix1</Prefix>
         <Encryption>
            <SSE-KMS>
               <KeyId>
               arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab
               </KeyId>
            </SSE-KMS>
         </Encryption>
      </S3BucketDestination>
   </Destination>
   <Schedule>
      <Frequency>Daily</Frequency>
   </Schedule>
   <IncludedObjectVersions>All</IncludedObjectVersions>
   <OptionalFields>
      <Field>Size</Field>
      <Field>LastModifiedDate</Field>
      <Field>ETag</Field>
      <Field>StorageClass</Field>
      <Field>IsMultipartUploaded</Field>
      <Field>ReplicationStatus</Field>
      <Field>EncryptionStatus</Field>
      <Field>ObjectLockRetainUntilDate</Field>
      <Field>ObjectLockMode</Field>
      <Field>ObjectLockLegalHoldStatus</Field>
   </OptionalFields>
</InventoryConfiguration>
""".encode('utf8')

# conforms to schema and only using supported fields and values
SUPPORTED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<InventoryConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Id>0</Id>
   <IsEnabled>true</IsEnabled>
   <Destination>
      <S3BucketDestination>
         <Format>Parquet</Format>
         <Bucket>arn:aws:s3:::destination-bucket</Bucket>
      </S3BucketDestination>
   </Destination>
   <Schedule>
      <Frequency>Daily</Frequency>
   </Schedule>
   <IncludedObjectVersions>Current</IncludedObjectVersions>
</InventoryConfiguration>
""".encode('utf8')

# does not conform to schema
INVALID_XML = """<?xml version="1.0" encoding="UTF-8"?>
<InventoryConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <Id>0</Id>
   <IsEnabled>true</IsEnabled>
   <Destination>
      <S3BucketDestination>
         <Bucket>arn:aws:s3:::destination-bucket</Bucket>
      </S3BucketDestination>
   </Destination>
   <Schedule>
      <Frequency>Daily</Frequency>
   </Schedule>
   <IncludedObjectVersions>All</IncludedObjectVersions>
</InventoryConfiguration>
""".encode('utf8')


class TestInventoryConfiguration(S3ApiTestCase):
    def test_from_xml_conforms_but_unsupported(self):
        conf = InventoryConfiguration.from_xml(UNSUPPORTED_XML, validate=False)
        self.assertEqual('destination-bucket',
                         conf.dest_bucket)
        self.assertEqual(True, conf.enabled)
        self.assertEqual('Daily', conf.schedule)
        self.assertEqual('CSV', conf.fmt)
        self.assertEqual('report1', conf.inventory_id)
        self.assertEqual('All', conf.include_versions)

        with self.assertRaises(DocumentInvalid) as cm:
            InventoryConfiguration.from_xml(UNSUPPORTED_XML, validate=True)
        errors = str(cm.exception).split('\n')
        self.assertEqual(
            {'Destination/S3BucketDestination/AccountId is not supported',
             'Destination/S3BucketDestination/Encryption is not supported',
             'Destination/S3BucketDestination/Format must be Parquet',
             'Destination/S3BucketDestination/Prefix is not supported',
             'Filter is not supported',
             'Id must be 0',
             'IncludedObjectVersions must be Current',
             'OptionalFields is not supported'},
            set(errors))

    def test_from_xml_conforms_and_supported(self):
        def verify(conf):
            self.assertEqual('destination-bucket',
                             conf.dest_bucket)
            self.assertEqual(True, conf.enabled)
            self.assertEqual('Daily', conf.schedule)
            self.assertEqual('Parquet', conf.fmt)
            self.assertEqual('0', conf.inventory_id)
            self.assertEqual('Current', conf.include_versions)
        verify(InventoryConfiguration.from_xml(SUPPORTED_XML))
        verify(InventoryConfiguration.from_xml(SUPPORTED_XML, validate=True))

    def test_from_xml_non_conformant(self):
        with self.assertRaises(DocumentInvalid) as cm:
            InventoryConfiguration.from_xml(INVALID_XML, validate=False)
        errors = str(cm.exception).split('\n')
        self.assertEqual(
            {'Expecting an element Format, got nothing, line 6'},
            set(errors))


class TestS3ApiInventory(S3ApiTestCase):
    def test_GET_feature_disabled(self):
        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('501', status.split()[0])
        self.assertEqual([], self.swift.calls_with_headers)
        self.assertIn(b'Not implemented.', body)

    def test_GET_inventory_not_configured(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        req = Request.blank('/bucket?inventory&id=not-configured',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('404', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(2, len(calls), calls)
        self.assertEqual([call[0] for call in calls], ['HEAD'] * 2)
        self.assertIn(b'The specified configuration does not exist.', body)

    def test_GET_inventory_enabled(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'destination-bucket',
            'modified_time': 123456.789,
            'enabled': True,
            'deleted': False,
        }
        self.make_app()
        resp_headers = {
            'X-Container-Sysmeta-Inventory-0-Config': json.dumps(config)
        }
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, resp_headers, None)

        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('200', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(2, len(calls), calls)
        self.assertEqual([call[0] for call in calls], ['HEAD'] * 2)
        elem = fromstring(body, 'InventoryConfiguration')
        self.assertEqual('true', elem.find('./IsEnabled').text)
        self.assertEqual('Daily', elem.find('./Schedule/Frequency').text)
        self.assertEqual(
            'arn:aws:s3:::destination-bucket',
            elem.find('./Destination/S3BucketDestination/Bucket').text)
        self.assertEqual(
            'Parquet',
            elem.find('./Destination/S3BucketDestination/Format').text)
        self.assertEqual('Current', elem.find('./IncludedObjectVersions').text)

    def test_GET_inventory_deleted(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'destination-bucket',
            'modified_time': 123456.789,
            'enabled': True,
            'deleted': True,
        }
        self.make_app()
        resp_headers = {
            'X-Container-Sysmeta-Inventory-0-Config': json.dumps(config)
        }
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, resp_headers, None)

        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('404', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(2, len(calls), calls)
        self.assertEqual([call[0] for call in calls], ['HEAD'] * 2)
        self.assertIn(b'The specified configuration does not exist.', body)

    def test_GET_inventory_list(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'arn:aws:s3:::destination-bucket',
            'modified_time': 123456.789,
            'enabled': True,
            'deleted': False,
        }
        self.make_app()
        resp_headers = {
            'X-Container-Sysmeta-Inventory-0-Config': json.dumps(config)
        }
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, resp_headers, None)

        req = Request.blank('/bucket?inventory&continuation-token=ignored',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('200', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(2, len(calls), calls)
        self.assertEqual([call[0] for call in calls], ['HEAD'] * 2)
        root = fromstring(body)
        self.assertEqual('ListInventoryConfigurationsResult', root.tag)
        self.assertIsNone(root.find('ContinuationToken'))
        elem = root.find('InventoryConfiguration')
        self.assertEqual('true', elem.find('./IsEnabled').text)
        self.assertEqual('Daily', elem.find('./Schedule/Frequency').text)
        self.assertEqual(
            'arn:aws:s3:::destination-bucket',
            elem.find('./Destination/S3BucketDestination/Bucket').text)
        self.assertEqual(
            'Parquet',
            elem.find('./Destination/S3BucketDestination/Format').text)
        self.assertEqual('Current', elem.find('./IncludedObjectVersions').text)
        self.assertEqual('false', root.find('./IsTruncated').text)

    def test_GET_inventory_list_deleted(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'arn:aws:s3:::destination-bucket',
            'modified_time': 123456.789,
            'enabled': True,
            'deleted': True,
        }
        self.make_app()
        resp_headers = {
            'X-Container-Sysmeta-Inventory-0-Config': json.dumps(config)
        }
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, resp_headers, None)

        req = Request.blank('/bucket?inventory&continuation-token=ignored',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('200', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(2, len(calls), calls)
        self.assertEqual([call[0] for call in calls], ['HEAD'] * 2)
        root = fromstring(body)
        self.assertEqual('ListInventoryConfigurationsResult', root.tag)
        self.assertIsNone(root.find('ContinuationToken'))
        self.assertIsNone(root.find('InventoryConfiguration'))
        self.assertEqual('false', root.find('./IsTruncated').text)

    def test_GET_inventory_list_empty(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        req = Request.blank('/bucket?inventory&continuation-token=ignored',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('200', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(2, len(calls), calls)
        self.assertEqual([call[0] for call in calls], ['HEAD'] * 2)
        root = fromstring(body)
        self.assertEqual('ListInventoryConfigurationsResult', root.tag)
        self.assertIsNone(root.find('ContinuationToken'))
        self.assertIsNone(root.find('InventoryConfiguration'))
        self.assertEqual('false', root.find('./IsTruncated').text)

    def test_PUT_feature_disabled(self):
        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('501', status.split()[0])
        self.assertEqual([], self.swift.calls_with_headers)

    def test_PUT_enable_inventory(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        self.swift.register(
            'POST', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=SUPPORTED_XML)
        with mock_timestamp_now() as ts:
            status, headers, body = self.call_s3api(req)
        self.assertEqual('204', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(1, len(calls), calls)
        self.assertEqual(calls[0][0], 'POST')
        exp_config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'destination-bucket',
            'modified_time': float(ts),
            'enabled': True,
            'deleted': False,
        }
        actual_hdrs = calls[0][2]
        self.assertIn('X-Container-Sysmeta-Inventory-0-Config', actual_hdrs)
        self.assertEqual(
            exp_config,
            json.loads(actual_hdrs['X-Container-Sysmeta-Inventory-0-Config']))

    def test_PUT_enable_inventory_abbreviated_dest_bucket(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        self.swift.register(
            'POST', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        xml = fromstring(SUPPORTED_XML,
                         'InventoryConfiguration')
        xml.find('./Destination/S3BucketDestination/Bucket').text = 'd-bucket'
        body = tostring(xml)

        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=body)
        with mock_timestamp_now() as ts:
            status, headers, body = self.call_s3api(req)
        self.assertEqual('204', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(1, len(calls), calls)
        self.assertEqual(calls[0][0], 'POST')
        exp_config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'd-bucket',
            'modified_time': float(ts),
            'enabled': True,
            'deleted': False,
        }
        actual_hdrs = calls[0][2]
        self.assertIn('X-Container-Sysmeta-Inventory-0-Config', actual_hdrs)
        self.assertEqual(
            exp_config,
            json.loads(actual_hdrs['X-Container-Sysmeta-Inventory-0-Config']))

    def test_PUT_disable_inventory(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        self.swift.register(
            'POST', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./IsEnabled').text = 'false'
        body = tostring(xml)

        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=body)
        with mock_timestamp_now() as ts:
            status, headers, body = self.call_s3api(req)
        self.assertEqual('204', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(1, len(calls), calls)
        self.assertEqual(calls[0][0], 'POST')
        exp_config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'destination-bucket',
            'modified_time': float(ts),
            'enabled': False,
            'deleted': False,
        }
        actual_hdrs = calls[0][2]
        self.assertIn('X-Container-Sysmeta-Inventory-0-Config', actual_hdrs)
        self.assertEqual(
            exp_config,
            json.loads(actual_hdrs['X-Container-Sysmeta-Inventory-0-Config']))

    def test_PUT_missing_fields(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=INVALID_XML)
        with mock_timestamp_now():
            status, headers, body = self.call_s3api(req)
        self.assertEqual('400', status.split()[0])
        self.assertEqual([], self.swift.calls_with_headers)
        self.assertEqual(
            "<?xml version='1.0' encoding='UTF-8'?>\n"
            "<Error><Code>MalformedXML</Code><Message>The XML you provided "
            "was not well-formed or did not validate against our published "
            "schema: Expecting an element Format, got nothing, line 6"
            "</Message></Error>".encode('utf8'), body)

    def test_DELETE_inventory(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'arn:aws:s3:::destination-bucket',
            'modified_time': 123456.789,
            'enabled': True,
            'deleted': False,
        }
        self.make_app()
        resp_headers = {
            'X-Container-Sysmeta-Inventory-0-Config': json.dumps(config)
        }
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, resp_headers, None)

        req = Request.blank('/bucket?inventory&id=0',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=None)
        with mock_timestamp_now() as ts:
            status, headers, body = self.call_s3api(req)
        self.assertEqual('204', status.split()[0])

        calls = self.swift.calls_with_headers
        self.assertEqual(3, len(calls), calls)
        self.assertEqual(calls[2][0], 'POST')
        exp_config = {
            'period': 'Daily',
            'source': 's3api',
            'dest_container': 'destination-bucket',
            'modified_time': float(ts),
            'enabled': False,
            'deleted': True,
        }
        actual_hdrs = calls[2][2]
        self.assertIn('X-Container-Sysmeta-Inventory-0-Config', actual_hdrs)
        self.assertEqual(
            exp_config,
            json.loads(actual_hdrs['X-Container-Sysmeta-Inventory-0-Config']))

        # non-existent config
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        req = Request.blank('/bucket?inventory&id=non-existent',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=None)
        with mock_timestamp_now() as ts:
            status, headers, body = self.call_s3api(req)
        self.assertEqual('404', status.split()[0])
        self.assertIn(b'The specified configuration does not exist.', body)

    def test_PUT_unsupported_inventory_id(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()

        req = Request.blank(
            '/bucket?inventory&id=unsupported',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()},
            body=SUPPORTED_XML)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('400', status.split()[0])

    def test_PUT_valid_fields(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()

        def do_test(method, body):
            req = Request.blank(
                '/bucket?inventory&id=0',
                environ={'REQUEST_METHOD': method},
                headers={'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()},
                body=body)
            status, _, _ = self.call_s3api(req)
            self.assertEqual('204', status.split()[0])

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Schedule/Frequency').text = 'Daily'
        body = tostring(xml)
        do_test('PUT', body)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Schedule/Frequency').text = 'Weekly'
        body = tostring(xml)
        do_test('PUT', body)

    def test_PUT_invalid_fields(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()

        def do_test(method, body):
            req = Request.blank(
                '/bucket?inventory&id=0',
                environ={'REQUEST_METHOD': method},
                headers={'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()},
                body=body)
            status, _, _ = self.call_s3api(req)
            self.assertEqual('400', status.split()[0])

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Schedule/Frequency').text = 'Monthly'
        body = tostring(xml)
        do_test('PUT', body)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Schedule/Frequency').text = 'Hourly'
        body = tostring(xml)
        do_test('PUT', body)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Destination/S3BucketDestination/Format').text = 'CSV'
        body = tostring(xml)
        do_test('PUT', body)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Destination/S3BucketDestination/Format').text = 'ORC'
        body = tostring(xml)
        do_test('PUT', body)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./Id').text = 'not_0'
        body = tostring(xml)
        do_test('PUT', body)

        xml = fromstring(SUPPORTED_XML, 'InventoryConfiguration')
        xml.find('./IncludedObjectVersions').text = 'All'
        body = tostring(xml)
        do_test('PUT', body)

    def test_allowed_paths(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.swift.register(
            'POST', '/v1/AUTH_test/bucket', HTTPNoContent, {}, None)

        def do_test(method, allowed_paths):
            self.conf['s3_inventory_allowed_paths'] = allowed_paths
            self.make_app()
            req = Request.blank(
                '/bucket?inventory&id=0',
                environ={'REQUEST_METHOD': method},
                headers={'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()},
                body=SUPPORTED_XML)
            status, headers, body = self.call_s3api(req)
            return status.split()[0]

        self.assertEqual('400', do_test('PUT', 'bucketX'))
        self.assertEqual('400', do_test('PUT', 'bucketX*'))
        self.assertEqual('204', do_test('PUT', 'bucket'))
        self.assertEqual('204', do_test('PUT', 'buck*'))
        self.assertEqual('204', do_test('PUT', 'b*'))
        self.assertEqual('204', do_test('PUT', '*'))

    def test_bucket_does_not_exist_404(self):
        self.conf['s3_inventory_enabled'] = 'yes'
        self.make_app()
        self.swift.register('POST', '/v1/AUTH_test/not-a-bucket',
                            HTTPNotFound, {}, None)
        self.swift.register('GET', '/v1/AUTH_test/not-a-bucket',
                            HTTPNotFound, {}, None)

        def do_test(method):
            req = Request.blank(
                '/not-a-bucket?inventory&id=0',
                environ={'REQUEST_METHOD': method},
                headers={'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()},
                body=SUPPORTED_XML)
            status, headers, body = self.call_s3api(req)
            return status.split()[0]

        self.assertEqual('404', do_test('PUT'))
        self.assertEqual('404', do_test('DELETE'))
        self.assertEqual('404', do_test('GET'))


if __name__ == '__main__':
    unittest.main()
