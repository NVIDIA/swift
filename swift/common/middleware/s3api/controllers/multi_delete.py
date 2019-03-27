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

import copy
import json

from swift.cli.container_deleter import make_delete_jobs
from swift.common.constraints import MAX_OBJECT_NAME_LENGTH
from swift.common.utils import public, StreamingPile, Timestamp, \
    config_true_value
from swift.common.wsgi import make_pre_authed_request

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation
from swift.common.middleware.s3api.etree import Element, SubElement, \
    fromstring, tostring, XMLSyntaxError, DocumentInvalid
from swift.common.middleware.s3api.s3response import HTTPOk, \
    S3NotImplemented, NoSuchKey, ErrorResponse, MalformedXML, \
    UserKeyMustBeSpecified, AccessDenied, MissingRequestBodyError, \
    ServiceUnavailable


class MultiObjectDeleteController(Controller):
    """
    Handles Delete Multiple Objects, which is logged as a MULTI_OBJECT_DELETE
    operation in the S3 server log.
    """
    def _gen_error_body(self, error, elem, delete_list):
        for key, version in delete_list:
            if version is not None:
                # TODO: how to return versionId on bucket does not exist error?
                raise S3NotImplemented()

            error_elem = SubElement(elem, 'Error')
            SubElement(error_elem, 'Key').text = key
            SubElement(error_elem, 'Code').text = error.__class__.__name__
            SubElement(error_elem, 'Message').text = error._msg

        return tostring(elem)

    @public
    @bucket_operation
    def POST(self, req):
        """
        Handles Delete Multiple Objects.
        """
        def object_key_iter(elem):
            for obj in elem.iterchildren('Object'):
                key = obj.find('./Key').text
                if not key:
                    raise UserKeyMustBeSpecified()
                version = obj.find('./VersionId')
                if version is not None:
                    version = version.text

                yield key, version

        max_body_size = min(
            # FWIW, AWS limits multideletes to 1000 keys, and swift limits
            # object names to 1024 bytes (by default). Add a factor of two to
            # allow some slop.
            2 * self.conf.max_multi_delete_objects * MAX_OBJECT_NAME_LENGTH,
            # But, don't let operators shoot themselves in the foot
            10 * 1024 * 1024)

        try:
            xml = req.xml(max_body_size)
            if not xml:
                raise MissingRequestBodyError()

            req.check_md5(xml)
            elem = fromstring(xml, 'Delete', self.logger)

            quiet = elem.find('./Quiet')
            if quiet is not None and quiet.text.lower() == 'true':
                self.quiet = True
            else:
                self.quiet = False

            delete_list = list(object_key_iter(elem))
            if len(delete_list) > self.conf.max_multi_delete_objects:
                raise MalformedXML()
        except (XMLSyntaxError, DocumentInvalid):
            raise MalformedXML()
        except ErrorResponse:
            raise
        except Exception as e:
            self.logger.error(e)
            raise

        elem = Element('DeleteResult')

        # check bucket existence
        try:
            req.get_response(self.app, 'HEAD')
        except AccessDenied as error:
            body = self._gen_error_body(error, elem, delete_list)
            return HTTPOk(body=body)

        def do_delete(base_req, key, version):
            req = copy.copy(base_req)
            req.environ = copy.copy(base_req.environ)
            req.object_name = key
            if version:
                req.params = {'version-id': version, 'symlink': 'get'}

            try:
                try:
                    query = req.gen_multipart_manifest_delete_query(
                        self.app, version=version)
                except NoSuchKey:
                    query = {}
                if version:
                    query['version-id'] = version
                    query['symlink'] = 'get'

                resp = req.get_response(self.app, method='DELETE', query=query,
                                        headers={'Accept': 'application/json'})
                # Have to read the response to actually do the SLO delete
                if query.get('multipart-manifest'):
                    try:
                        delete_result = json.loads(resp.body)
                        if delete_result['Errors']:
                            # NB: bulk includes 404s in "Number Not Found",
                            # not "Errors"
                            msg_parts = [delete_result['Response Status']]
                            msg_parts.extend(
                                '%s: %s' % (obj, status)
                                for obj, status in delete_result['Errors'])
                            return key, {'code': 'SLODeleteError',
                                         'message': '\n'.join(msg_parts)}
                        # else, all good
                    except (ValueError, TypeError, KeyError):
                        # Logs get all the gory details
                        self.logger.exception(
                            'Could not parse SLO delete response: %r',
                            resp.body)
                        # Client gets something more generic
                        return key, {'code': 'SLODeleteError',
                                     'message': 'Unexpected swift response'}
            except NoSuchKey:
                pass
            except ErrorResponse as e:
                return key, {'code': e.__class__.__name__, 'message': e._msg}
            except Exception:
                self.logger.exception(
                    'Unexpected Error handling DELETE of %r %r' % (
                        req.container_name, key))
                return key, {'code': 'Server Error', 'message': 'Server Error'}

            return key, None

        if self.conf.use_async_delete:
            container_info = req.get_container_info(self.app)
            versions_enabled = config_true_value(container_info.get(
                'sysmeta', {}).get('versions-enabled'))
            if versions_enabled or any(
                    version is not None for _key, version in delete_list):
                raise S3NotImplemented()
            # Fire off first delete inline to check that we're authed to delete
            _, err = do_delete(req, *delete_list[0])
            if err:
                # We've already ruled out multipart uploads and S3-style acls;
                # assume that whatever this failure was would apply to all of
                # the objects.
                for key, _version in delete_list:
                    error = SubElement(elem, 'Error')
                    SubElement(error, 'Key').text = key
                    SubElement(error, 'Code').text = err['code']
                    SubElement(error, 'Message').text = err['message']
                return HTTPOk(body=tostring(elem))
            # Note that even on success, we *also* async-delete this first key.
            # This ensures the container listing gets updated immediately.

            ts = Timestamp.now()
            dest_container_body = []
            dest_storage_policy = container_info['storage_policy']
            for key, _version in delete_list:
                # NB: no ROWIDs
                dest_container_body.append({
                    "name": key, "deleted": 1, "created_at": ts.internal,
                    "storage_policy_index": dest_storage_policy,
                    "etag": "noetag",
                    "content_type": "application/deleted", "size": 0})
                if not self.quiet:
                    deleted = SubElement(elem, 'Deleted')
                    SubElement(deleted, 'Key').text = key

            # make UPDATE request to bulk-insert expirer jobs
            exp_jobs = make_delete_jobs(
                req.account,
                req.container_name,
                [key for key, version in delete_list if version is None],
                ts)
            # TODO: extend with jobs for specific versions
            enqueue_req = make_pre_authed_request(
                req.environ,
                method='UPDATE',
                path="/v1/.expiring_objects/%d" % int(ts),
                body=json.dumps(exp_jobs),
                headers={'Content-Type': 'application/json',
                         'X-Backend-Storage-Policy-Index': '0',
                         'X-Backend-Allow-Private-Methods': 'True'},
            )
            resp = enqueue_req.get_response(self.app)
            if not resp.is_success:
                self.logger.error(
                    'Failed to enqueue expiration entries: %s\n%s',
                    resp.status, resp.body)
                return ServiceUnavailable()
            # consume the response (should be short)
            resp.body

            # make UPDATE request to the container so we can
            # update the listing *now*
            resp = req.get_response(
                self.app, method='UPDATE',
                body=json.dumps(dest_container_body),
                headers={
                    'Content-Type': 'application/json',
                    'X-Backend-Storage-Policy-Index': str(dest_storage_policy),
                    'X-Backend-Allow-Private-Methods': 'True'})
            if not resp.is_success:
                return resp
            # consume the response (should be short)
            resp.body

            return HTTPOk(body=tostring(elem))

        # else, do inline deletes
        with StreamingPile(self.conf.multi_delete_concurrency) as pile:
            for key, err in pile.asyncstarmap(do_delete, (
                    (req, key, version) for key, version in delete_list)):
                if err:
                    error = SubElement(elem, 'Error')
                    SubElement(error, 'Key').text = key
                    SubElement(error, 'Code').text = err['code']
                    SubElement(error, 'Message').text = err['message']
                elif not self.quiet:
                    deleted = SubElement(elem, 'Deleted')
                    SubElement(deleted, 'Key').text = key

        body = tostring(elem)

        return HTTPOk(body=body)
