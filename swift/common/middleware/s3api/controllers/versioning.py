# Copyright (c) 2010-2014 OpenStack Foundation.
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

from swift.common.utils import public

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation
from swift.common.middleware.s3api.etree import Element, tostring, \
    fromstring, XMLSyntaxError, DocumentInvalid, SubElement
from swift.common.middleware.s3api.s3response import HTTPOk, MalformedXML

MAX_PUT_VERSIONING_BODY_SIZE = 10240


class VersioningController(Controller):
    """
    Handles the following APIs:

    * GET Bucket versioning
    * PUT Bucket versioning

    Those APIs are logged as VERSIONING operations in the S3 server log.
    """
    @public
    @bucket_operation
    def GET(self, req):
        """
        Handles GET Bucket versioning.
        """
        sysmeta = req.get_container_info(self.app).get('sysmeta', {})

        status = {
            'True': 'Enabled',
            'False': 'Suspended',
            None: None,
        }[sysmeta.get('versions-enabled')]
        elem = Element('VersioningConfiguration')
        if status:
            SubElement(elem, 'Status').text = status
        body = tostring(elem)

        return HTTPOk(body=body, content_type=None)

    @public
    @bucket_operation
    def PUT(self, req):
        """
        Handles PUT Bucket versioning.
        """
        xml = req.xml(MAX_PUT_VERSIONING_BODY_SIZE)
        try:
            elem = fromstring(xml, 'VersioningConfiguration')
            status = elem.find('./Status').text
        except (XMLSyntaxError, DocumentInvalid):
            raise MalformedXML()
        except Exception as e:
            self.logger.error(e)
            raise

        if status not in ['Enabled', 'Suspended']:
            raise MalformedXML()

        # Set up versioning
        # NB: object_versioning responsible for ensuring its container exists
        if status == 'Enabled':
            req.headers['X-Versions-Enabled'] = 'true'
        else:
            req.headers['X-Versions-Enabled'] = 'false'
        # Set the container back to what it originally was
        req.get_response(self.app, 'POST')

        return HTTPOk()
