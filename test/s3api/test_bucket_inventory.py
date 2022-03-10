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
from botocore.exceptions import ClientError

from test.s3api import BaseS3TestCase


class TestBucketInventory(BaseS3TestCase):

    maxDiff = None

    def setUp(self):
        self.client = self.get_s3_client(1)
        self.bucket_name = self.create_name('inventory')
        resp = self.client.create_bucket(Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.client = self.get_s3_client(1)
        self.inventory_configuration = {
            'Destination': {
                'S3BucketDestination': {
                    'Bucket': 'arn:aws:s3:::%s' % self.bucket_name,
                    'Format': 'Parquet',
                }
            },
            'IsEnabled': True,
            'Id': '0',
            'IncludedObjectVersions': 'Current',
            'Schedule': {
                'Frequency': 'Daily'
            }
        }

    def tearDown(self):
        self.clear_bucket(self.client, self.bucket_name)
        super(TestBucketInventory, self).tearDown()

    def test_inventory_configuration(self):
        with self.assertRaises(ClientError) as cm:
            self.client.get_bucket_inventory_configuration(
                Bucket=self.bucket_name,
                Id='0',)
        self.assertIn('The specified configuration does not exist.',
                      str(cm.exception))

        resp = self.client.list_bucket_inventory_configurations(
            Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('InventoryConfigurationList', resp)

        # enable
        resp = self.client.put_bucket_inventory_configuration(
            Bucket=self.bucket_name, Id='0',
            InventoryConfiguration=self.inventory_configuration)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        resp = self.client.get_bucket_inventory_configuration(
            Bucket=self.bucket_name, Id='0')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(self.inventory_configuration,
                         resp['InventoryConfiguration'])

        resp = self.client.list_bucket_inventory_configurations(
            Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual([self.inventory_configuration],
                         resp['InventoryConfigurationList'])

        # disable
        disabled_config = dict(self.inventory_configuration, IsEnabled=False)
        resp = self.client.put_bucket_inventory_configuration(
            Bucket=self.bucket_name, Id='0',
            InventoryConfiguration=disabled_config)
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        resp = self.client.get_bucket_inventory_configuration(
            Bucket=self.bucket_name, Id='0')
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(disabled_config,
                         resp['InventoryConfiguration'])

        resp = self.client.list_bucket_inventory_configurations(
            Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual([disabled_config],
                         resp['InventoryConfigurationList'])

        # DELETE inventory config
        resp = self.client.delete_bucket_inventory_configuration(
            Bucket=self.bucket_name, Id='0')
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        with self.assertRaises(ClientError) as cm:
            self.client.get_bucket_inventory_configuration(
                Bucket=self.bucket_name, Id='0')
        self.assertIn('The specified configuration does not exist.',
                      str(cm.exception))

        resp = self.client.list_bucket_inventory_configurations(
            Bucket=self.bucket_name)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertNotIn('InventoryConfigurationList', resp)
