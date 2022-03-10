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
import botocore
from botocore.exceptions import ClientError

import test.functional as tf
from test.functional.s3api import S3ApiBaseBoto3


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


class TestS3ApiBucketInventory(S3ApiBaseBoto3):
    inventory_configuration = {
        'Destination': {
            'S3BucketDestination': {
                'Bucket': 'dest-bucket',
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

    def setUp(self):
        if not tf.cluster_info['s3api'].get('s3_inventory_enabled', False):
            raise tf.SkipTest('s3 bucket inventory is not enabled')
        super(TestS3ApiBucketInventory, self).setUp()

    def test_bucket_inventory(self):
        bucket = 'mybucket'
        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # PUT inventory config
        resp = self.conn.put_bucket_inventory_configuration(
            Bucket=bucket,
            Id='0',
            InventoryConfiguration=self.inventory_configuration
        )
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # GET inventory config
        resp = self.conn.get_bucket_inventory_configuration(
            Bucket=bucket,
            Id='0'
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            {
                'IsEnabled': True,
                'Destination': {
                    'S3BucketDestination': {
                        'Bucket': 'arn:aws:s3:::dest-bucket',
                        'Format': 'Parquet'
                    }
                },
                'IncludedObjectVersions': 'Current',
                'Id': '0',
                'Schedule': {'Frequency': 'Daily'}
            },
            resp['InventoryConfiguration']
        )

        # GET inventory config list
        resp = self.conn.list_bucket_inventory_configurations(
            Bucket=bucket,
            ContinuationToken='ignored'
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual(
            [
                {
                    'IsEnabled': True,
                    'Destination': {
                        'S3BucketDestination': {
                            'Bucket': 'arn:aws:s3:::dest-bucket',
                            'Format': 'Parquet'
                        }
                    },
                    'IncludedObjectVersions': 'Current',
                    'Id': '0',
                    'Schedule': {'Frequency': 'Daily'}
                },
            ],
            resp['InventoryConfigurationList']
        )
        self.assertNotIn('ContinuationToken', resp)
        self.assertEqual(False, resp['IsTruncated'])

        # DELETE inventory config
        resp = self.conn.delete_bucket_inventory_configuration(
            Bucket=bucket,
            Id='0'
        )
        self.assertEqual(204, resp['ResponseMetadata']['HTTPStatusCode'])

        # GET inventory config list
        resp = self.conn.list_bucket_inventory_configurations(
            Bucket=bucket,
            ContinuationToken='ignored'
        )
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        self.assertEqual([], resp.get('InventoryConfigurationList', []))
        self.assertNotIn('ContinuationToken', resp)
        self.assertEqual(False, resp['IsTruncated'])

    def test_GET_bucket_inventory_config_does_not_exist(self):
        bucket = 'mybucket'
        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # GET non-existent inventory config
        with self.assertRaises(ClientError) as cm:
            self.conn.get_bucket_inventory_configuration(
                Bucket=bucket,
                Id='0'
            )
        self.assertIn('The specified configuration does not exist.',
                      str(cm.exception))

        # GET inventory config unsupported id
        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.conn.get_bucket_inventory_configuration(
                Bucket=bucket,
                Id='unsupported_id'
            )
        self.assertEqual(
            404, cm.exception.response['ResponseMetadata']['HTTPStatusCode'])

    def test_DELETE_bucket_inventory_config_does_not_exist(self):
        bucket = 'mybucket'
        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])
        # DELETE non-existent inventory config
        with self.assertRaises(ClientError) as cm:
            self.conn.delete_bucket_inventory_configuration(
                Bucket=bucket,
                Id='0'
            )
        self.assertIn('The specified configuration does not exist.',
                      str(cm.exception))

        # DELETE inventory config unsupported id
        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.conn.delete_bucket_inventory_configuration(
                Bucket=bucket,
                Id='unsupported_id'
            )
        self.assertEqual(
            404, cm.exception.response['ResponseMetadata']['HTTPStatusCode'])

    def test_PUT_bucket_inventory_invalid_request(self):
        bucket = 'mybucket'
        # PUT Bucket
        resp = self.conn.create_bucket(Bucket=bucket)
        self.assertEqual(200, resp['ResponseMetadata']['HTTPStatusCode'])

        # PUT inventory config unsupported id
        with self.assertRaises(botocore.exceptions.ClientError) as cm:
            self.conn.put_bucket_inventory_configuration(
                Bucket=bucket,
                Id='unsupported_id',
                InventoryConfiguration=self.inventory_configuration
            )
        self.assertEqual(
            400, cm.exception.response['ResponseMetadata']['HTTPStatusCode'])
