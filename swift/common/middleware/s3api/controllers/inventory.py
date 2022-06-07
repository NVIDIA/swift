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

from swift.common.utils import public, config_true_value

from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.etree import fromstring, \
    XMLSyntaxError, DocumentInvalid, Element, SubElement, tostring
from swift.common.middleware.s3api.s3response import HTTPOk, \
    S3NotImplemented, MalformedXML, InvalidRequest, HTTPNoContent, \
    ErrorResponse

MAX_PUT_INVENTORY_BODY_SIZE = 10240
# currently only support a single inventory configuration with id '0'
DEFAULT_INVENTORY_ID = '0'


class NoSuchInventoryConfiguration(ErrorResponse):
    _status = '404 Not Found'
    _msg = 'The specified configuration does not exist.'


class InventoryConfiguration(object):
    required = {
        './IsEnabled': 'enabled',
        './Destination/S3BucketDestination/Bucket': 'dest_bucket',
        './Destination/S3BucketDestination/Format': 'fmt',
        './Id': 'inventory_id',
        './Schedule/Frequency': 'schedule',
        'IncludedObjectVersions': 'include_versions',
    }
    unsupported = [
        'Destination/S3BucketDestination/Prefix',
        'Destination/S3BucketDestination/AccountId',
        'Destination/S3BucketDestination/Encryption',
        'Filter',
        'OptionalFields',
    ]

    def __init__(self,
                 dest_bucket='',
                 enabled=True,
                 schedule='Daily',
                 fmt='Parquet',
                 inventory_id=DEFAULT_INVENTORY_ID,
                 include_versions='Current',
                 deleted=False
                 ):
        self.dest_bucket = dest_bucket.split(':', 6)[-1]
        self.enabled = config_true_value(enabled)
        self.schedule = schedule
        self.fmt = fmt
        self.inventory_id = inventory_id
        self.include_versions = include_versions
        self.deleted = deleted

    @property
    def schedule(self):
        return self._schedule

    @schedule.setter
    def schedule(self, value):
        value = str(value)
        if value == '86400':
            self._schedule = 'Daily'
        elif value == '604800':
            self._schedule = 'Weekly'
        elif value.title() in ('Daily', 'Weekly'):
            self._schedule = value.title()
        else:
            # tolerate other internally managed setting
            self._schedule = 'Unknown'

    @classmethod
    def from_xml(cls, xml, validate=True):
        # load xml and check against s3 api schema...
        config = fromstring(xml, 'InventoryConfiguration')
        kwargs = {}
        for path, key in cls.required.items():
            value = config.find(path).text
            kwargs[key] = value

        conf = cls(**kwargs)

        if validate:
            errors = []
            # the schema is relaxed to allow swift to return 'Unknown' schedule
            # in case we have a config with period other than daily or weekly,
            # but we don't want to accept 'Unknown' from clients
            if conf.schedule not in ('Daily', 'Weekly'):
                errors.append('Invalid value for Schedule/Frequency')
            # swift backend does not support some optional config elements...
            for path in cls.unsupported:
                if config.find(path) is not None:
                    errors.append('%s is not supported' % path)
            # swift backend is opinionated about some required config values...
            if conf.fmt != 'Parquet':
                errors.append(
                    'Destination/S3BucketDestination/Format must be Parquet')
            if conf.inventory_id != DEFAULT_INVENTORY_ID:
                errors.append(
                    'Id must be 0')
            if conf.include_versions != 'Current':
                errors.append('IncludedObjectVersions must be Current')
            if errors:
                raise DocumentInvalid('\n'.join(errors))
        return conf

    @classmethod
    def from_json(cls, data):
        return cls(
            dest_bucket=data['dest_container'],
            enabled=data.get('enabled', True),  # backwards compatible
            schedule=data['period'],
            deleted=data.get('deleted', False),  # backwards compatible
        )

    def to_xml(self):
        elem = Element('InventoryConfiguration')
        SubElement(elem, 'IsEnabled').text = str(self.enabled).lower()
        SubElement(elem, 'Id').text = str(self.inventory_id)
        SubElement(elem, 'IncludedObjectVersions').text = self.include_versions
        dest = SubElement(elem, 'Destination')
        s3dest = SubElement(dest, 'S3BucketDestination')
        SubElement(s3dest, 'Bucket').text = ('arn:aws:s3:::%s'
                                             % self.dest_bucket)
        SubElement(s3dest, 'Format').text = self.fmt
        schedule = SubElement(elem, 'Schedule')
        SubElement(schedule, 'Frequency').text = self.schedule
        return elem

    def to_dict(self):
        return {
            'period': self.schedule,
            'dest_container': self.dest_bucket,
            'source': 's3api',
            'deleted': self.deleted,
            'enabled': self.enabled,
        }


def check_config_id(req, config=None):
    config_id = req.params.get('id')
    if config_id is None:
        raise InvalidRequest('Missing required id parameter.')
    if config_id != DEFAULT_INVENTORY_ID:
        raise InvalidRequest(
            'Only requests with id parameter equal to 0 are supported.')
    if config and config.inventory_id != config_id:
        return InvalidRequest(
            'The request id parameter must equal the InventoryConfiguration '
            'Id.')
    return config_id


def add_sysmeta_header(req, config):
    config_data = config.to_dict()
    config_data['modified_time'] = float(req.ensure_x_timestamp())
    key = 'X-Container-Sysmeta-Inventory-%s-Config' % config.inventory_id
    req.headers[key] = json.dumps(config_data)


class InventoryController(Controller):
    def __init__(self, app, conf, logger, **kwargs):
        super(InventoryController, self).__init__(app, conf, logger, **kwargs)

    def check_path(self, req):
        if req.account:
            path = '/'.join([req.account, req.container_name])
        else:
            path = req.container_name

        for allowed in self.conf.s3_inventory_allowed_paths:
            if (path == allowed
                    or (allowed.endswith('*')
                        and path.startswith(allowed[:-1]))):
                break
        else:
            raise InvalidRequest(
                'Inventory configuration is not supported for this bucket')

    def check_req(self, req):
        if not (req.conf.s3_inventory_enabled and req.is_bucket_request):
            # Handle Object ACL
            raise S3NotImplemented()
        self.check_path(req)

    def config_from_sysmeta(self, req, inventory_id):
        sysmeta = req.get_container_info(self.app).get('sysmeta', {})
        key = 'inventory-%s-config' % inventory_id
        config_data = json.loads(sysmeta.get(key, '{}'))
        if config_data:
            try:
                return InventoryConfiguration.from_json(config_data)
            except ValueError as err:
                self.logger.warning(
                    'invalid inventory config sysmeta for %s: %s',
                    req.path, err)
                pass
        return None

    @public
    def PUT(self, req):
        """
        Handles PUT inventory configuration
        """
        self.check_req(req)

        xml = req.xml(MAX_PUT_INVENTORY_BODY_SIZE)
        try:
            config = InventoryConfiguration.from_xml(xml, validate=True)
        except (XMLSyntaxError, DocumentInvalid) as e:
            msg = (
                'The XML you provided was not well-formed or did not validate '
                'against our published schema: %s' % e
            )
            raise MalformedXML(msg=msg)
        except Exception as e:
            self.logger.error(e)
            raise

        check_config_id(req, config)
        add_sysmeta_header(req, config)
        req.get_response(self.app, 'POST')
        # The s3 api doc says this returns 200 but cross-compatibility tests
        # show it is a 204
        return HTTPNoContent()

    @public
    def DELETE(self, req):
        """
        Handles DELETE inventory configuration
        """
        self.check_req(req)
        # note: no need to validate the id - if it is not the supported id then
        # we'll just return 404
        config_id = req.params.get('id')
        config = self.config_from_sysmeta(req, config_id)
        if config:
            config.deleted = True
            config.enabled = False
            add_sysmeta_header(req, config)
            req.get_response(self.app, 'POST')
            response = HTTPNoContent()
        else:
            response = NoSuchInventoryConfiguration()

        return response

    @public
    def GET(self, req):
        """
        Handles GET inventory configuration
        """
        self.check_req(req)

        listing = False
        # note: no need to validate the id - if it is not the supported id then
        # we'll just return 404
        req_config_id = req.params.get('id')
        if req_config_id is None:
            listing = True
            # backend only supports one config to list...
            req_config_id = DEFAULT_INVENTORY_ID

        config = self.config_from_sysmeta(req, req_config_id)
        if listing:
            # note: a continuation token is not expected since we never
            # truncate a listing, but we ignore continuation token anyway.
            xml = Element('ListInventoryConfigurationsResult')
            if config and not config.deleted:
                xml.append(config.to_xml())
            SubElement(xml, 'IsTruncated').text = 'false'
            response = HTTPOk(body=tostring(xml),
                              content_type='application/xml')
        elif config and not config.deleted:
            xml = config.to_xml()
            response = HTTPOk(body=tostring(xml),
                              content_type='application/xml')
        else:
            response = NoSuchInventoryConfiguration()

        return response
