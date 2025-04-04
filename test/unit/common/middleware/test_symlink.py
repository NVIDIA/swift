#!/usr/bin/env python
# Copyright (c) 2016 OpenStack Foundation
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

import unittest
import io
import json
from unittest import mock

from urllib.parse import parse_qs
from swift.common import swob
from swift.common.middleware import symlink, copy, versioned_writes, \
    listing_formats
from swift.common.swob import Request
from swift.common.request_helpers import get_reserved_name
from swift.common.utils import MD5_OF_EMPTY_STRING
from swift.common.registry import get_swift_info
from test.unit.common.middleware.helpers import FakeSwift
from test.unit.common.middleware.test_versioned_writes import FakeCache


class TestSymlinkMiddlewareBase(unittest.TestCase):
    def setUp(self):
        self.app = FakeSwift()
        self.sym = symlink.filter_factory({
            'symloop_max': '2',
        })(self.app)
        self.sym.logger = self.app.logger

    def call_app(self, req, app=None, expect_exception=False):
        if app is None:
            app = self.app

        self.authorized = []

        def authorize(req):
            self.authorized.append(req)

        if 'swift.authorize' not in req.environ:
            req.environ['swift.authorize'] = authorize

        status = [None]
        headers = [None]

        def start_response(s, h, ei=None):
            status[0] = s
            headers[0] = h

        body_iter = app(req.environ, start_response)
        body = b''
        caught_exc = None
        try:
            for chunk in body_iter:
                body += chunk
        except Exception as exc:
            if expect_exception:
                caught_exc = exc
            else:
                raise

        if expect_exception:
            return status[0], headers[0], body, caught_exc
        else:
            return status[0], headers[0], body

    def call_sym(self, req, **kwargs):
        return self.call_app(req, app=self.sym, **kwargs)


class TestSymlinkMiddleware(TestSymlinkMiddlewareBase):

    def test_symlink_info(self):
        swift_info = get_swift_info()
        self.assertEqual(swift_info['symlink'], {
            'symloop_max': 2,
            'static_links': True,
        })

    def test_symlink_simple_put(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'c1/o'},
                            body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o' % MD5_OF_EMPTY_STRING)
        self.assertEqual('application/symlink', hdrs.get('Content-Type'))

    def test_symlink_simple_put_with_content_type(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'c1/o',
                                     'Content-Type': 'application/linkyfoo'},
                            body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o' % MD5_OF_EMPTY_STRING)
        self.assertEqual('application/linkyfoo', hdrs.get('Content-Type'))

    def test_symlink_simple_put_with_etag(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'Etag': 'tgt-etag', 'Content-Length': 42,
            'Content-Type': 'application/foo'})
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'tgt-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[1]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o; '
                         'symlink_target_etag=tgt-etag; '
                         'symlink_target_bytes=42' % MD5_OF_EMPTY_STRING)
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        self.assertEqual('application/foo',
                         self.app.call_list[-1].headers['Content-Type'])

    def test_symlink_simple_put_with_quoted_etag(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'Etag': 'tgt-etag', 'Content-Length': 42,
            'Content-Type': 'application/foo'})
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': '"tgt-etag"',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[1]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o; '
                         'symlink_target_etag=tgt-etag; '
                         'symlink_target_bytes=42' % MD5_OF_EMPTY_STRING)
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        self.assertEqual('application/foo',
                         self.app.call_list[-1].headers['Content-Type'])

    def test_symlink_simple_put_with_etag_target_missing_content_type(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'Etag': 'tgt-etag', 'Content-Length': 42})
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'tgt-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[1]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o; '
                         'symlink_target_etag=tgt-etag; '
                         'symlink_target_bytes=42' % MD5_OF_EMPTY_STRING)
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        # N.B. the ObjectController would call _update_content_type on PUT
        # regardless, but you actually can't get a HEAD response without swob
        # setting a Content-Type
        self.assertEqual('text/html; charset=UTF-8',
                         self.app.call_list[-1].headers['Content-Type'])

    def test_symlink_simple_put_with_etag_explicit_content_type(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'Etag': 'tgt-etag', 'Content-Length': 42,
            'Content-Type': 'application/foo'})
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'tgt-etag',
                                'Content-Type': 'application/bar',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[1]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o; '
                         'symlink_target_etag=tgt-etag; '
                         'symlink_target_bytes=42' % MD5_OF_EMPTY_STRING)
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        self.assertEqual('application/bar',
                         self.app.call_list[-1].headers['Content-Type'])

    def test_symlink_simple_put_with_unmatched_etag(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'Etag': 'tgt-etag', 'Content-Length': 42})
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'not-tgt-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        self.assertIn(('Content-Location', '/v1/a/c1/o'), headers)
        self.assertEqual(body, b"Object Etag 'tgt-etag' does not match "
                         b"X-Symlink-Target-Etag header 'not-tgt-etag'")

    def test_symlink_simple_put_to_non_existing_object(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'not-tgt-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        self.assertIn(('Content-Location', '/v1/a/c1/o'), headers)
        self.assertIn(b'does not exist', body)

    def test_symlink_simple_put_error(self):
        self.app.register('HEAD', '/v1/a/c1/o',
                          swob.HTTPInternalServerError, {}, 'bad news')
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'not-tgt-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '500 Internal Error')
        # this is a PUT response; so if we have a content-length...
        self.assertGreater(int(dict(headers)['Content-Length']), 0)
        # ... we better have a body!
        self.assertIn(b'Internal Error', body)

    def test_symlink_simple_put_to_non_existing_object_override(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPNotFound, {})
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'some-tgt-etag',
                                # this header isn't normally sent with PUT
                                'X-Symlink-Target-Bytes': '13',
                            }, body='')
        # this can be set in container_sync
        req.environ['swift.symlink_override'] = True
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')

    def test_symlink_put_with_prevalidated_etag(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT', headers={
            'X-Symlink-Target': 'c1/o',
            'X-Object-Sysmeta-Symlink-Target-Etag': 'tgt-etag',
            'X-Object-Sysmeta-Symlink-Target-Bytes': '13',
            'Content-Type': 'application/foo',
        }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')

        self.assertEqual([
            # N.B. no HEAD!
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        self.assertEqual('application/foo',
                         self.app.call_list[-1].headers['Content-Type'])

        method, path, hdrs = self.app.calls_with_headers[0]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o; '
                         'symlink_target_etag=tgt-etag; '
                         'symlink_target_bytes=13' % MD5_OF_EMPTY_STRING)

    def test_symlink_put_with_prevalidated_etag_sysmeta_incomplete(self):
        req = Request.blank('/v1/a/c/symlink', method='PUT', headers={
            'X-Symlink-Target': 'c1/o',
            'X-Object-Sysmeta-Symlink-Target-Etag': 'tgt-etag',
        }, body='')
        with self.assertRaises(KeyError) as cm:
            self.call_sym(req)
        self.assertEqual(cm.exception.args[0], swob.header_to_environ_key(
            'X-Object-Sysmeta-Symlink-Target-Bytes'))

    def test_symlink_chunked_put(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'c1/o'},
                            environ={'wsgi.input': io.BytesIO(b'')})
        self.assertIsNone(req.content_length)  # sanity
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertNotIn('X-Object-Sysmeta-Symlink-Target-Account', hdrs)
        val = hdrs.get('X-Object-Sysmeta-Container-Update-Override-Etag')
        self.assertEqual(val, '%s; symlink_target=c1/o' % MD5_OF_EMPTY_STRING)

    def test_symlink_chunked_put_error(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'c1/o'},
                            environ={'wsgi.input':
                                     io.BytesIO(b'this has a body')})
        self.assertIsNone(req.content_length)  # sanity
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '400 Bad Request')

    def test_symlink_put_different_account(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'c1/o',
                                     'X-Symlink-Target-Account': 'a1'},
                            body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertEqual(hdrs.get('X-Object-Sysmeta-Symlink-Target-Account'),
                         'a1')

    def test_symlink_put_leading_slash(self):
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': '/c1/o'},
                            body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '412 Precondition Failed')
        self.assertEqual(body, b"X-Symlink-Target header must be of "
                               b"the form <container name>/<object name>")

    def test_symlink_put_non_zero_length(self):
        req = Request.blank('/v1/a/c/symlink', method='PUT', body='req_body',
                            headers={'X-Symlink-Target': 'c1/o'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b'Symlink requests require a zero byte body')

    def test_symlink_put_bad_object_header(self):
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'o'},
                            body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, "412 Precondition Failed")
        self.assertEqual(body, b"X-Symlink-Target header must be of "
                               b"the form <container name>/<object name>")

    def test_symlink_put_bad_account_header(self):
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={'X-Symlink-Target': 'c1/o',
                                     'X-Symlink-Target-Account': 'a1/c1'},
                            body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, "412 Precondition Failed")
        self.assertEqual(body, b"Account name cannot contain slashes")

    def test_get_symlink(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Meta-Color': 'Red'})
        req = Request.blank('/v1/a/c/symlink?symlink=get', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertIsInstance(headers, list)
        self.assertIn(('X-Symlink-Target', 'c1/o'), headers)
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertIn(('X-Object-Meta-Color', 'Red'), headers)
        self.assertEqual(body, b'')
        # HEAD with same params find same registered GET
        req = Request.blank('/v1/a/c/symlink?symlink=get', method='HEAD')
        head_status, head_headers, head_body = self.call_sym(req)
        self.assertEqual(head_status, '200 OK')
        self.assertIn(('X-Symlink-Target', 'c1/o'), head_headers)
        self.assertNotIn('X-Symlink-Target-Account', dict(head_headers))
        self.assertIn(('X-Object-Meta-Color', 'Red'), head_headers)
        self.assertEqual(head_body, b'')

    def test_get_symlink_with_account(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        req = Request.blank('/v1/a/c/symlink?symlink=get', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Symlink-Target', 'c1/o'), headers)
        self.assertIn(('X-Symlink-Target-Account', 'a2'), headers)

    def test_get_symlink_not_found(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a/c/symlink', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '404 Not Found')
        self.assertNotIn('Content-Location', dict(headers))

    def test_get_target_object(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('GET', '/v1/a2/c1/o', swob.HTTPOk, {}, 'resp_body')
        req_headers = {'X-Newest': 'True', 'X-Backend-Something': 'future'}
        req = Request.blank('/v1/a/c/symlink', method='GET',
                            headers=req_headers)
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, b'resp_body')
        self.assertNotIn('X-Symlink-Target', dict(headers))
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertIn(('Content-Location', '/v1/a2/c1/o'), headers)
        calls = self.app.call_list
        req_headers.update({
            'Host': 'localhost:80',
            'X-Backend-Ignore-Range-If-Metadata-Present':
            'x-object-sysmeta-symlink-target',
        })
        self.assertEqual(req_headers, calls[0].headers)
        req_headers['User-Agent'] = 'Swift'
        self.assertEqual(req_headers, calls[1].headers)
        self.assertFalse(calls[2:])
        self.assertFalse(self.app.unread_requests)

    def test_get_target_object_not_found(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-account': 'a2'})
        self.app.register('GET', '/v1/a2/c1/o', swob.HTTPNotFound, {}, '')
        req = Request.blank('/v1/a/c/symlink', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '404 Not Found')
        self.assertEqual(body, b'')
        self.assertNotIn('X-Symlink-Target', dict(headers))
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertIn(('Content-Location', '/v1/a2/c1/o'), headers)
        self.assertFalse(self.app.unread_requests)

    def test_get_target_object_range_not_satisfiable(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('GET', '/v1/a2/c1/o',
                          swob.HTTPRequestedRangeNotSatisfiable, {}, '')
        req = Request.blank('/v1/a/c/symlink', method='GET',
                            headers={'Range': 'bytes=1-2'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '416 Requested Range Not Satisfiable')
        self.assertEqual(
            body, b'<html><h1>Requested Range Not Satisfiable</h1>'
                  b'<p>The Range requested is not available.</p></html>')
        self.assertNotIn('X-Symlink-Target', dict(headers))
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertIn(('Content-Location', '/v1/a2/c1/o'), headers)
        self.assertFalse(self.app.unread_requests)

    def test_get_ec_symlink_range_unsatisfiable_can_redirect_to_target(self):
        self.app.register('GET', '/v1/a/c/symlink',
                          swob.HTTPRequestedRangeNotSatisfiable,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('GET', '/v1/a2/c1/o', swob.HTTPOk,
                          {'Content-Range': 'bytes 1-2/10'}, 'es')
        req = Request.blank('/v1/a/c/symlink', method='GET',
                            headers={'Range': 'bytes=1-2'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, b'es')
        self.assertNotIn('X-Symlink-Target', dict(headers))
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertIn(('Content-Location', '/v1/a2/c1/o'), headers)
        self.assertIn(('Content-Range', 'bytes 1-2/10'), headers)

    def test_get_non_symlink(self):
        # this is not symlink object
        self.app.register('GET', '/v1/a/c/obj', swob.HTTPOk, {}, 'resp_body')
        req = Request.blank('/v1/a/c/obj', method='GET')

        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body, b'resp_body')

        # Assert special headers for symlink are not in response
        self.assertNotIn('X-Symlink-Target', dict(headers))
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertNotIn('Content-Location', dict(headers))

    def test_get_static_link_mismatched_etag(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Etag': 'the-etag'})
        # apparently target object was overwritten
        self.app.register('GET', '/v1/a/c1/o', swob.HTTPOk,
                          {'ETag': 'not-the-etag'}, 'resp_body')
        req = Request.blank('/v1/a/c/symlink', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        self.assertEqual(body, b"Object Etag 'not-the-etag' does not "
                         b"match X-Symlink-Target-Etag header 'the-etag'")

    def test_get_static_link_to_symlink(self):
        self.app.register('GET', '/v1/a/c/static_link', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/symlink',
                           'X-Object-Sysmeta-Symlink-Target-Etag': 'the-etag'})
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'ETag': 'the-etag',
                           'X-Object-Sysmeta-Symlink-Target': 'c1/o'})
        self.app.register('GET', '/v1/a/c1/o', swob.HTTPOk,
                          {'ETag': 'not-the-etag'}, 'resp_body')
        req = Request.blank('/v1/a/c/static_link', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')

    def test_get_static_link_to_symlink_fails(self):
        self.app.register('GET', '/v1/a/c/static_link', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/symlink',
                           'X-Object-Sysmeta-Symlink-Target-Etag': 'the-etag'})
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'ETag': 'not-the-etag',
                           'X-Object-Sysmeta-Symlink-Target': 'c1/o'})
        req = Request.blank('/v1/a/c/static_link', method='GET')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        self.assertEqual(body, b"X-Symlink-Target-Etag headers do not match")

    def put_static_link_to_symlink(self):
        self.app.register('HEAD', '/v1/a/c/symlink', swob.HTTPOk,
                          {'ETag': 'symlink-etag',
                           'X-Object-Sysmeta-Symlink-Target': 'c/o',
                           'Content-Type': 'application/symlink'})
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk,
                          {'ETag': 'tgt-etag',
                           'Content-Type': 'application/data'}, 'resp_body')
        self.app.register('PUT', '/v1/a/c/static_link', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/static_link', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c/symlink',
                                'X-Symlink-Target-Etag': 'symlink-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual([], self.app.calls)
        self.assertEqual('application/data',
                         self.app.call_list[-1].headers['Content-Type'])

    def test_head_symlink(self):
        self.app.register('HEAD', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Meta-Color': 'Red'})
        req = Request.blank('/v1/a/c/symlink?symlink=get', method='HEAD')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Symlink-Target', 'c1/o'), headers)
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertIn(('X-Object-Meta-Color', 'Red'), headers)

    def test_head_symlink_with_account(self):
        self.app.register('HEAD', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2',
                           'X-Object-Meta-Color': 'Red'})
        req = Request.blank('/v1/a/c/symlink?symlink=get', method='HEAD')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('X-Symlink-Target', 'c1/o'), headers)
        self.assertIn(('X-Symlink-Target-Account', 'a2'), headers)
        self.assertIn(('X-Object-Meta-Color', 'Red'), headers)

    def test_head_target_object(self):
        # this test is also validating that the symlink metadata is not
        # returned, but the target object metadata does return
        self.app.register('HEAD', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2',
                           'X-Object-Meta-Color': 'Red'})
        self.app.register('HEAD', '/v1/a2/c1/o', swob.HTTPOk,
                          {'X-Object-Meta-Color': 'Green'})
        req_headers = {'X-Newest': 'True', 'X-Backend-Something': 'future'}
        req = Request.blank('/v1/a/c/symlink', method='HEAD',
                            headers=req_headers)
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertNotIn('X-Symlink-Target', dict(headers))
        self.assertNotIn('X-Symlink-Target-Account', dict(headers))
        self.assertNotIn(('X-Object-Meta-Color', 'Red'), headers)
        self.assertIn(('X-Object-Meta-Color', 'Green'), headers)
        self.assertIn(('Content-Location', '/v1/a2/c1/o'), headers)
        calls = self.app.call_list
        req_headers.update({
            'Host': 'localhost:80',
            'X-Backend-Ignore-Range-If-Metadata-Present':
            'x-object-sysmeta-symlink-target',
        })
        self.assertEqual(req_headers, calls[0].headers)
        req_headers['User-Agent'] = 'Swift'
        self.assertEqual(req_headers, calls[1].headers)
        self.assertFalse(calls[2:])

    def test_get_symlink_to_reserved_object(self):
        cont = get_reserved_name('versioned')
        obj = get_reserved_name('symlink', '9999998765.99999')
        symlink_target = "%s/%s" % (cont, obj)
        version_path = '/v1/a/%s' % symlink_target
        self.app.register('GET', '/v1/a/versioned/symlink', swob.HTTPOk, {
            symlink.TGT_OBJ_SYSMETA_SYMLINK_HDR: symlink_target,
            symlink.ALLOW_RESERVED_NAMES: 'true',
            'x-object-sysmeta-symlink-target-etag': MD5_OF_EMPTY_STRING,
            'x-object-sysmeta-symlink-target-bytes': '0',
        })
        self.app.register('GET', version_path, swob.HTTPOk, {})
        req = Request.blank('/v1/a/versioned/symlink', headers={
            'Range': 'foo', 'If-Match': 'bar'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('Content-Location', version_path), headers)
        self.assertEqual(len(self.authorized), 1)
        self.assertNotIn('X-Backend-Allow-Reserved-Names',
                         self.app.calls_with_headers[0])
        call_headers = self.app.call_list[1].headers
        self.assertEqual('true', call_headers[
            'X-Backend-Allow-Reserved-Names'])
        self.assertEqual('foo', call_headers['Range'])
        self.assertEqual('bar', call_headers['If-Match'])

    def test_get_symlink_to_reserved_symlink(self):
        cont = get_reserved_name('versioned')
        obj = get_reserved_name('symlink', '9999998765.99999')
        symlink_target = "%s/%s" % (cont, obj)
        version_path = '/v1/a/%s' % symlink_target
        self.app.register('GET', '/v1/a/versioned/symlink', swob.HTTPOk, {
            symlink.TGT_OBJ_SYSMETA_SYMLINK_HDR: symlink_target,
            symlink.ALLOW_RESERVED_NAMES: 'true',
            'x-object-sysmeta-symlink-target-etag': MD5_OF_EMPTY_STRING,
            'x-object-sysmeta-symlink-target-bytes': '0',
        })
        self.app.register('GET', version_path, swob.HTTPOk, {
            symlink.TGT_OBJ_SYSMETA_SYMLINK_HDR: 'unversioned/obj',
            'ETag': MD5_OF_EMPTY_STRING,
        })
        self.app.register('GET', '/v1/a/unversioned/obj', swob.HTTPOk, {
        })
        req = Request.blank('/v1/a/versioned/symlink')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertIn(('Content-Location', '/v1/a/unversioned/obj'), headers)
        self.assertEqual(len(self.authorized), 2)

    def test_symlink_too_deep(self):
        self.app.register('GET', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/sym1'})
        self.app.register('GET', '/v1/a/c/sym1', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/sym2'})
        self.app.register('GET', '/v1/a/c/sym2', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/o'})
        req = Request.blank('/v1/a/c/symlink', method='HEAD')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        self.assertEqual(body, b'')
        req = Request.blank('/v1/a/c/symlink')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        self.assertEqual(body, b'Too many levels of symbolic links, '
                         b'maximum allowed is 2')

    def test_symlink_change_symloopmax(self):
        # similar test to test_symlink_too_deep, but now changed the limit to 3
        self.sym = symlink.filter_factory({
            'symloop_max': '3',
        })(self.app)
        self.sym.logger = self.app.logger
        self.app.register('HEAD', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/sym1'})
        self.app.register('HEAD', '/v1/a/c/sym1', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/sym2'})
        self.app.register('HEAD', '/v1/a/c/sym2', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/o',
                           'X-Object-Meta-Color': 'Red'})
        self.app.register('HEAD', '/v1/a/c/o', swob.HTTPOk,
                          {'X-Object-Meta-Color': 'Green'})
        req = Request.blank('/v1/a/c/symlink', method='HEAD')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')

        # assert that the correct metadata was returned
        self.assertNotIn(('X-Object-Meta-Color', 'Red'), headers)
        self.assertIn(('X-Object-Meta-Color', 'Green'), headers)

    def test_sym_to_sym_to_target(self):
        # this test is also validating that the symlink metadata is not
        # returned, but the target object metadata does return
        self.app.register('HEAD', '/v1/a/c/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c/sym1',
                           'X-Object-Meta-Color': 'Red'})
        self.app.register('HEAD', '/v1/a/c/sym1', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Meta-Color': 'Yellow'})
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk,
                          {'X-Object-Meta-Color': 'Green'})
        req = Request.blank('/v1/a/c/symlink', method='HEAD')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        self.assertNotIn(('X-Symlink-Target', 'c1/o'), headers)
        self.assertNotIn(('X-Symlink-Target-Account', 'a2'), headers)
        self.assertNotIn(('X-Object-Meta-Color', 'Red'), headers)
        self.assertNotIn(('X-Object-Meta-Color', 'Yellow'), headers)
        self.assertIn(('X-Object-Meta-Color', 'Green'), headers)
        self.assertIn(('Content-Location', '/v1/a/c1/o'), headers)

    def test_symlink_post(self):
        self.app.register('POST', '/v1/a/c/symlink', swob.HTTPAccepted,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o'})
        req = Request.blank('/v1/a/c/symlink', method='POST',
                            headers={'X-Object-Meta-Color': 'Red'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '307 Temporary Redirect')
        self.assertEqual(
            body,
            b'The requested POST was applied to a symlink. POST '
            b'directly to the target to apply requested metadata.')
        method, path, hdrs = self.app.calls_with_headers[0]
        val = hdrs.get('X-Object-Meta-Color')
        self.assertEqual(val, 'Red')

    def test_non_symlink_post(self):
        self.app.register('POST', '/v1/a/c/o', swob.HTTPAccepted, {})
        req = Request.blank('/v1/a/c/o', method='POST',
                            headers={'X-Object-Meta-Color': 'Red'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '202 Accepted')

    def test_set_symlink_POST_fail(self):
        # Setting a link with a POST request is not allowed
        req = Request.blank('/v1/a/c/o', method='POST',
                            headers={'X-Symlink-Target': 'c1/regular_obj'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(body, b"A PUT request is required to set a symlink "
                         b"target")

    def test_symlink_post_but_fail_at_server(self):
        self.app.register('POST', '/v1/a/c/o', swob.HTTPNotFound, {})
        req = Request.blank('/v1/a/c/o', method='POST',
                            headers={'X-Object-Meta-Color': 'Red'})
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '404 Not Found')

    def test_validate_and_prep_request_headers(self):
        def do_test(headers):
            req = Request.blank('/v1/a/c/o', method='PUT',
                                headers=headers)
            symlink._validate_and_prep_request_headers(req)

        # normal cases
        do_test({'X-Symlink-Target': 'c1/o1'})
        do_test({'X-Symlink-Target': 'c1/sub/o1'})
        do_test({'X-Symlink-Target': 'c1%2Fo1'})
        # specify account
        do_test({'X-Symlink-Target': 'c1/o1',
                 'X-Symlink-Target-Account': 'another'})
        # URL encoded is safe
        do_test({'X-Symlink-Target': 'c1%2Fo1'})
        # URL encoded + multibytes is also safe
        target = u'\u30b0\u30e9\u30d6\u30eb/\u30a2\u30ba\u30ec\u30f3'
        target = swob.bytes_to_wsgi(target.encode('utf8'))
        do_test({'X-Symlink-Target': target})
        do_test({'X-Symlink-Target': swob.wsgi_quote(target)})

        target = swob.bytes_to_wsgi(u'\u30b0\u30e9\u30d6\u30eb'.encode('utf8'))
        do_test(
            {'X-Symlink-Target': 'cont/obj',
             'X-Symlink-Target-Account': target})
        do_test(
            {'X-Symlink-Target': 'cont/obj',
             'X-Symlink-Target-Account': swob.wsgi_quote(target)})

    def test_validate_and_prep_request_headers_invalid_format(self):
        def do_test(headers, status, err_msg):
            req = Request.blank('/v1/a/c/o', method='PUT',
                                headers=headers)
            with self.assertRaises(swob.HTTPException) as cm:
                symlink._validate_and_prep_request_headers(req)

            self.assertEqual(cm.exception.status, status)
            self.assertEqual(cm.exception.body, err_msg)

        do_test({'X-Symlink-Target': '/c1/o1'},
                '412 Precondition Failed',
                b'X-Symlink-Target header must be of the '
                b'form <container name>/<object name>')
        do_test({'X-Symlink-Target': 'c1o1'},
                '412 Precondition Failed',
                b'X-Symlink-Target header must be of the '
                b'form <container name>/<object name>')
        do_test({'X-Symlink-Target': 'c1/o1',
                 'X-Symlink-Target-Account': '/another'},
                '412 Precondition Failed',
                b'Account name cannot contain slashes')
        do_test({'X-Symlink-Target': 'c1/o1',
                 'X-Symlink-Target-Account': 'an/other'},
                '412 Precondition Failed',
                b'Account name cannot contain slashes')
        # url encoded case
        do_test({'X-Symlink-Target': '%2Fc1%2Fo1'},
                '412 Precondition Failed',
                b'X-Symlink-Target header must be of the '
                b'form <container name>/<object name>')
        do_test({'X-Symlink-Target': 'c1/o1',
                 'X-Symlink-Target-Account': '%2Fanother'},
                '412 Precondition Failed',
                b'Account name cannot contain slashes')
        do_test({'X-Symlink-Target': 'c1/o1',
                 'X-Symlink-Target-Account': 'an%2Fother'},
                '412 Precondition Failed',
                b'Account name cannot contain slashes')
        # with multi-bytes
        target = u'/\u30b0\u30e9\u30d6\u30eb/\u30a2\u30ba\u30ec\u30f3'
        target = swob.bytes_to_wsgi(target.encode('utf8'))
        do_test(
            {'X-Symlink-Target': target},
            '412 Precondition Failed',
            b'X-Symlink-Target header must be of the '
            b'form <container name>/<object name>')
        do_test(
            {'X-Symlink-Target': swob.wsgi_quote(target)},
            '412 Precondition Failed',
            b'X-Symlink-Target header must be of the '
            b'form <container name>/<object name>')
        account = u'\u30b0\u30e9\u30d6\u30eb/\u30a2\u30ba\u30ec\u30f3'
        account = swob.bytes_to_wsgi(account.encode('utf8'))
        do_test(
            {'X-Symlink-Target': 'c/o',
             'X-Symlink-Target-Account': account},
            '412 Precondition Failed',
            b'Account name cannot contain slashes')
        do_test(
            {'X-Symlink-Target': 'c/o',
             'X-Symlink-Target-Account': swob.wsgi_quote(account)},
            '412 Precondition Failed',
            b'Account name cannot contain slashes')

    def test_validate_and_prep_request_headers_points_to_itself(self):
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'X-Symlink-Target': 'c/o'})
        with self.assertRaises(swob.HTTPException) as cm:
            symlink._validate_and_prep_request_headers(req)
        self.assertEqual(cm.exception.status, '400 Bad Request')
        self.assertEqual(cm.exception.body, b'Symlink cannot target itself')

        # Even if set account to itself, it will fail as well
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'X-Symlink-Target': 'c/o',
                                     'X-Symlink-Target-Account': 'a'})
        with self.assertRaises(swob.HTTPException) as cm:
            symlink._validate_and_prep_request_headers(req)
        self.assertEqual(cm.exception.status, '400 Bad Request')
        self.assertEqual(cm.exception.body, b'Symlink cannot target itself')

        # sanity, the case to another account is safe
        req = Request.blank('/v1/a/c/o', method='PUT',
                            headers={'X-Symlink-Target': 'c/o',
                                     'X-Symlink-Target-Account': 'a1'})
        symlink._validate_and_prep_request_headers(req)

    def test_symloop_max_config(self):
        self.app = FakeSwift()
        # sanity
        self.sym = symlink.filter_factory({
            'symloop_max': '1',
        })(self.app)
        self.assertEqual(self.sym.symloop_max, 1)
        # < 1 case will result in default
        self.sym = symlink.filter_factory({
            'symloop_max': '-1',
        })(self.app)
        self.assertEqual(self.sym.symloop_max, symlink.DEFAULT_SYMLOOP_MAX)


class SymlinkCopyingTestCase(TestSymlinkMiddlewareBase):
    # verify interaction of copy and symlink middlewares

    def setUp(self):
        self.app = FakeSwift()
        conf = {'symloop_max': '2'}
        self.sym = symlink.filter_factory(conf)(self.app)
        self.sym.logger = self.app.logger
        self.copy = copy.filter_factory({})(self.sym)

    def call_copy(self, req, **kwargs):
        return self.call_app(req, app=self.copy, **kwargs)

    def test_copy_symlink_target(self):
        req = Request.blank('/v1/a/src_cont/symlink', method='COPY',
                            headers={'Destination': 'tgt_cont/tgt_obj'})
        self._test_copy_symlink_target(req)
        req = Request.blank('/v1/a/tgt_cont/tgt_obj', method='PUT',
                            headers={'X-Copy-From': 'src_cont/symlink'})
        self._test_copy_symlink_target(req)

    def _test_copy_symlink_target(self, req):
        self.app.register('GET', '/v1/a/src_cont/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('GET', '/v1/a2/c1/o', swob.HTTPOk, {}, 'resp_body')
        self.app.register('PUT', '/v1/a/tgt_cont/tgt_obj', swob.HTTPCreated,
                          {}, 'resp_body')
        status, headers, body = self.call_copy(req)
        method, path, hdrs = self.app.calls_with_headers[0]
        self.assertEqual(method, 'GET')
        self.assertEqual(path, '/v1/a/src_cont/symlink')
        self.assertEqual('/src_cont/symlink', hdrs.get('X-Copy-From'))
        method, path, hdrs = self.app.calls_with_headers[1]
        self.assertEqual(method, 'GET')
        self.assertEqual(path, '/v1/a2/c1/o')
        self.assertEqual('/src_cont/symlink', hdrs.get('X-Copy-From'))
        method, path, hdrs = self.app.calls_with_headers[2]
        self.assertEqual(method, 'PUT')
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        # this is raw object copy
        self.assertEqual(val, None)
        self.assertEqual(status, '201 Created')

    def test_copy_symlink(self):
        req = Request.blank(
            '/v1/a/src_cont/symlink?symlink=get', method='COPY',
            headers={'Destination': 'tgt_cont/tgt_obj'})
        self._test_copy_symlink(req)
        req = Request.blank(
            '/v1/a/tgt_cont/tgt_obj?symlink=get', method='PUT',
            headers={'X-Copy-From': 'src_cont/symlink'})
        self._test_copy_symlink(req)

    def _test_copy_symlink(self, req):
        self.app.register('GET', '/v1/a/src_cont/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('PUT', '/v1/a/tgt_cont/tgt_obj', swob.HTTPCreated,
                          {'X-Symlink-Target': 'c1/o',
                           'X-Symlink-Target-Account': 'a2'})
        status, headers, body = self.call_copy(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        self.assertEqual(method, 'GET')
        self.assertEqual(path, '/v1/a/src_cont/symlink?symlink=get')
        self.assertEqual('/src_cont/symlink', hdrs.get('X-Copy-From'))
        method, path, hdrs = self.app.calls_with_headers[1]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertEqual(
            hdrs.get('X-Object-Sysmeta-Symlink-Target-Account'), 'a2')

    def test_copy_symlink_new_target(self):
        req = Request.blank(
            '/v1/a/src_cont/symlink?symlink=get', method='COPY',
            headers={'Destination': 'tgt_cont/tgt_obj',
                     'X-Symlink-Target': 'new_cont/new_obj',
                     'X-Symlink-Target-Account': 'new_acct'})
        self._test_copy_symlink_new_target(req)
        req = Request.blank(
            '/v1/a/tgt_cont/tgt_obj?symlink=get', method='PUT',
            headers={'X-Copy-From': 'src_cont/symlink',
                     'X-Symlink-Target': 'new_cont/new_obj',
                     'X-Symlink-Target-Account': 'new_acct'})
        self._test_copy_symlink_new_target(req)

    def _test_copy_symlink_new_target(self, req):
        self.app.register('GET', '/v1/a/src_cont/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('PUT', '/v1/a/tgt_cont/tgt_obj', swob.HTTPCreated,
                          {'X-Symlink-Target': 'c1/o',
                           'X-Symlink-Target-Account': 'a2'})
        status, headers, body = self.call_copy(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        self.assertEqual(method, 'GET')
        self.assertEqual(path, '/v1/a/src_cont/symlink?symlink=get')
        self.assertEqual('/src_cont/symlink', hdrs.get('X-Copy-From'))
        method, path, hdrs = self.app.calls_with_headers[1]
        self.assertEqual(method, 'PUT')
        self.assertEqual(path, '/v1/a/tgt_cont/tgt_obj?symlink=get')
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'new_cont/new_obj')
        self.assertEqual(hdrs.get('X-Object-Sysmeta-Symlink-Target-Account'),
                         'new_acct')

    def test_copy_symlink_with_slo_query(self):
        req = Request.blank(
            '/v1/a/src_cont/symlink?multipart-manifest=get&symlink=get',
            method='COPY', headers={'Destination': 'tgt_cont/tgt_obj'})
        self._test_copy_symlink_with_slo_query(req)
        req = Request.blank(
            '/v1/a/tgt_cont/tgt_obj?multipart-manifest=get&symlink=get',
            method='PUT', headers={'X-Copy-From': 'src_cont/symlink'})
        self._test_copy_symlink_with_slo_query(req)

    def _test_copy_symlink_with_slo_query(self, req):
        self.app.register('GET', '/v1/a/src_cont/symlink', swob.HTTPOk,
                          {'X-Object-Sysmeta-Symlink-Target': 'c1/o',
                           'X-Object-Sysmeta-Symlink-Target-Account': 'a2'})
        self.app.register('PUT', '/v1/a/tgt_cont/tgt_obj', swob.HTTPCreated,
                          {'X-Symlink-Target': 'c1/o',
                           'X-Symlink-Target-Account': 'a2'})
        status, headers, body = self.call_copy(req)
        self.assertEqual(status, '201 Created')
        method, path, hdrs = self.app.calls_with_headers[0]
        self.assertEqual(method, 'GET')
        path, query = path.split('?')
        query_dict = parse_qs(query)
        self.assertEqual(
            path, '/v1/a/src_cont/symlink')
        self.assertEqual(
            query_dict,
            {'multipart-manifest': ['get'], 'symlink': ['get'],
             'format': ['raw']})
        self.assertEqual('/src_cont/symlink', hdrs.get('X-Copy-From'))
        method, path, hdrs = self.app.calls_with_headers[1]
        val = hdrs.get('X-Object-Sysmeta-Symlink-Target')
        self.assertEqual(val, 'c1/o')
        self.assertEqual(
            hdrs.get('X-Object-Sysmeta-Symlink-Target-Account'), 'a2')

    def test_static_link_to_new_slo_manifest(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'X-Static-Large-Object': 'True',
            'Etag': 'manifest-etag',
            'X-Object-Sysmeta-Slo-Size': '1048576',
            'X-Object-Sysmeta-Slo-Etag': 'this-is-not-used',
            'Content-Length': 42,
            'Content-Type': 'application/big-data',
            'X-Object-Sysmeta-Container-Update-Override-Etag':
            '956859738870e5ca6aa17eeda58e4df0; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88',

        })
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'manifest-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        method, path, hdrs = self.app.calls_with_headers[-1]
        self.assertEqual('application/big-data', hdrs['Content-Type'])
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target'], 'c1/o')
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target-Etag'],
                         'manifest-etag')
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target-Bytes'],
                         '1048576')
        self.assertEqual(
            hdrs['X-Object-Sysmeta-Container-Update-Override-Etag'],
            'd41d8cd98f00b204e9800998ecf8427e; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88; '
            'symlink_target=c1/o; '
            'symlink_target_etag=manifest-etag; '
            'symlink_target_bytes=1048576')

    def test_static_link_to_old_slo_manifest(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'X-Static-Large-Object': 'True',
            'Etag': 'manifest-etag',
            'X-Object-Sysmeta-Slo-Size': '1048576',
            'X-Object-Sysmeta-Slo-Etag': '71e938d37c1d06dc634dd24660255a88',
            'Content-Length': 42,
            'Content-Type': 'application/big-data',

        })
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'manifest-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        method, path, hdrs = self.app.calls_with_headers[-1]
        self.assertEqual('application/big-data', hdrs['Content-Type'])
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target'], 'c1/o')
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target-Etag'],
                         'manifest-etag')
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target-Bytes'],
                         '1048576')
        self.assertEqual(
            hdrs['X-Object-Sysmeta-Container-Update-Override-Etag'],
            'd41d8cd98f00b204e9800998ecf8427e; '
            'slo_etag=71e938d37c1d06dc634dd24660255a88; '
            'symlink_target=c1/o; '
            'symlink_target_etag=manifest-etag; '
            'symlink_target_bytes=1048576')

    def test_static_link_to_really_old_slo_manifest(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'X-Static-Large-Object': 'True',
            'Etag': 'manifest-etag',
            'Content-Length': 42,
            'Content-Type': 'application/big-data',

        })
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'manifest-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '201 Created')
        self.assertEqual([
            ('HEAD', '/v1/a/c1/o'),
            ('PUT', '/v1/a/c/symlink'),
        ], self.app.calls)
        method, path, hdrs = self.app.calls_with_headers[-1]
        self.assertEqual('application/big-data', hdrs['Content-Type'])
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target'], 'c1/o')
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target-Etag'],
                         'manifest-etag')
        # symlink m/w is doing a HEAD, it's not going to going to read the
        # manifest body and sum up the bytes - so we just use manifest size
        self.assertEqual(hdrs['X-Object-Sysmeta-Symlink-Target-Bytes'],
                         '42')
        # no slo_etag, and target_bytes is manifest
        self.assertEqual(
            hdrs['X-Object-Sysmeta-Container-Update-Override-Etag'],
            'd41d8cd98f00b204e9800998ecf8427e; '
            'symlink_target=c1/o; '
            'symlink_target_etag=manifest-etag; '
            'symlink_target_bytes=42')

    def test_static_link_to_slo_manifest_slo_etag(self):
        self.app.register('HEAD', '/v1/a/c1/o', swob.HTTPOk, {
            'Etag': 'manifest-etag',
            'X-Object-Sysmeta-Slo-Etag': 'slo-etag',
            'Content-Length': 42,
        })
        self.app.register('PUT', '/v1/a/c/symlink', swob.HTTPCreated, {})
        # unquoted slo-etag doesn't match
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': 'slo-etag',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')
        # the quoted slo-etag is tolerated, but still doesn't match
        req = Request.blank('/v1/a/c/symlink', method='PUT',
                            headers={
                                'X-Symlink-Target': 'c1/o',
                                'X-Symlink-Target-Etag': '"slo-etag"',
                            }, body='')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '409 Conflict')


class SymlinkVersioningTestCase(TestSymlinkMiddlewareBase):
    # verify interaction of versioned_writes and symlink middlewares

    def setUp(self):
        self.app = FakeSwift()
        conf = {'symloop_max': '2'}
        self.sym = symlink.filter_factory(conf)(self.app)
        self.sym.logger = self.app.logger
        vw_conf = {'allow_versioned_writes': 'true'}
        self.vw = versioned_writes.filter_factory(vw_conf)(self.sym)

    def call_vw(self, req, **kwargs):
        return self.call_app(req, app=self.vw, **kwargs)

    def assertRequestEqual(self, req, other):
        self.assertEqual(req.method, other.method)
        self.assertEqual(req.path, other.path)

    def test_new_symlink_version_success(self):
        self.app.register(
            'PUT', '/v1/a/c/symlink', swob.HTTPCreated,
            {'X-Symlink-Target': 'new_cont/new_tgt',
             'X-Symlink-Target-Account': 'a'}, None)
        self.app.register(
            'GET', '/v1/a/c/symlink', swob.HTTPOk,
            {'last-modified': 'Thu, 1 Jan 1970 00:00:01 GMT',
             'X-Object-Sysmeta-Symlink-Target': 'old_cont/old_tgt',
             'X-Object-Sysmeta-Symlink-Target-Account': 'a'},
            '')
        self.app.register(
            'PUT', '/v1/a/ver_cont/007symlink/0000000001.00000',
            swob.HTTPCreated,
            {'X-Symlink-Target': 'old_cont/old_tgt',
             'X-Symlink-Target-Account': 'a'}, None)
        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/symlink',
            headers={'X-Symlink-Target': 'new_cont/new_tgt'},
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': cache,
                     'CONTENT_LENGTH': '0',
                     'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '201 Created')
        # authorized twice now because versioned_writes now makes a check on
        # PUT
        self.assertEqual(len(self.authorized), 2)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(['VW', 'VW', None], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[2]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/symlink', path)
        self.assertEqual(
            'new_cont/new_tgt',
            req_headers['X-Object-Sysmeta-Symlink-Target'])

    def test_delete_latest_version_no_marker_success(self):
        self.app.register(
            'GET',
            '/v1/a/ver_cont?prefix=003sym/&marker=&reverse=on',
            swob.HTTPOk, {},
            '[{"hash": "y", '
            '"last_modified": "2014-11-21T14:23:02.206740", '
            '"bytes": 0, '
            '"name": "003sym/2", '
            '"content_type": "text/plain"}, '
            '{"hash": "x", '
            '"last_modified": "2014-11-21T14:14:27.409100", '
            '"bytes": 0, '
            '"name": "003sym/1", '
            '"content_type": "text/plain"}]')
        self.app.register(
            'GET', '/v1/a/ver_cont/003sym/2', swob.HTTPCreated,
            {'content-length': '0',
             'X-Object-Sysmeta-Symlink-Target': 'c/tgt'}, None)
        self.app.register(
            'PUT', '/v1/a/c/sym', swob.HTTPCreated,
            {'X-Symlink-Target': 'c/tgt', 'X-Symlink-Target-Account': 'a'},
            None)
        self.app.register(
            'DELETE', '/v1/a/ver_cont/003sym/2', swob.HTTPOk,
            {}, None)

        cache = FakeCache({'sysmeta': {'versions-location': 'ver_cont'}})
        req = Request.blank(
            '/v1/a/c/sym',
            headers={'X-If-Delete-At': 1},
            environ={'REQUEST_METHOD': 'DELETE', 'swift.cache': cache,
                     'CONTENT_LENGTH': '0', 'swift.trans_id': 'fake_trans_id'})
        status, headers, body = self.call_vw(req)
        self.assertEqual(status, '200 OK')
        self.assertEqual(len(self.authorized), 1)
        self.assertRequestEqual(req, self.authorized[0])
        self.assertEqual(4, self.app.call_count)
        self.assertEqual(['VW', 'VW', 'VW', 'VW'], self.app.swift_sources)
        self.assertEqual({'fake_trans_id'}, set(self.app.txn_ids))
        calls = self.app.calls_with_headers
        method, path, req_headers = calls[2]
        self.assertEqual('PUT', method)
        self.assertEqual('/v1/a/c/sym', path)
        self.assertEqual(
            'c/tgt',
            req_headers['X-Object-Sysmeta-Symlink-Target'])


class TestSymlinkContainerContext(TestSymlinkMiddlewareBase):

    def setUp(self):
        super(TestSymlinkContainerContext, self).setUp()
        self.context = symlink.SymlinkContainerContext(
            self.sym.app, self.sym.logger)

    def test_extract_symlink_path_json_simple_etag(self):
        obj_dict = {"bytes": 6,
                    "last_modified": "1",
                    "hash": "etag",
                    "name": "obj",
                    "content_type": "application/octet-stream"}
        obj_dict = self.context._extract_symlink_path_json(
            obj_dict, 'v1', 'AUTH_a')
        self.assertEqual(obj_dict['hash'], 'etag')
        self.assertNotIn('symlink_path', obj_dict)

    def test_extract_symlink_path_json_symlink_path(self):
        obj_dict = {"bytes": 6,
                    "last_modified": "1",
                    "hash": "etag; symlink_target=c/o; something_else=foo; "
                    "symlink_target_etag=tgt_etag; symlink_target_bytes=8",
                    "name": "obj",
                    "content_type": "application/octet-stream"}
        obj_dict = self.context._extract_symlink_path_json(
            obj_dict, 'v1', 'AUTH_a')
        self.assertEqual(obj_dict['hash'], 'etag; something_else=foo')
        self.assertEqual(obj_dict['symlink_path'], '/v1/AUTH_a/c/o')
        self.assertEqual(obj_dict['symlink_etag'], 'tgt_etag')
        self.assertEqual(obj_dict['symlink_bytes'], 8)

    def test_extract_symlink_path_json_symlink_path_and_account(self):
        obj_dict = {
            "bytes": 6,
            "last_modified": "1",
            "hash": "etag; symlink_target=c/o; symlink_target_account=AUTH_a2",
            "name": "obj",
            "content_type": "application/octet-stream"}
        obj_dict = self.context._extract_symlink_path_json(
            obj_dict, 'v1', 'AUTH_a')
        self.assertEqual(obj_dict['hash'], 'etag')
        self.assertEqual(obj_dict['symlink_path'], '/v1/AUTH_a2/c/o')

    def test_extract_symlink_path_json_extra_key(self):
        obj_dict = {"bytes": 6,
                    "last_modified": "1",
                    "hash": "etag; symlink_target=c/o; extra_key=value",
                    "name": "obj",
                    "content_type": "application/octet-stream"}
        obj_dict = self.context._extract_symlink_path_json(
            obj_dict, 'v1', 'AUTH_a')
        self.assertEqual(obj_dict['hash'], 'etag; extra_key=value')
        self.assertEqual(obj_dict['symlink_path'], '/v1/AUTH_a/c/o')

    def test_get_container_simple(self):
        self.app.register(
            'GET',
            '/v1/a/c',
            swob.HTTPOk, {},
            json.dumps(
                [{"hash": "etag; symlink_target=c/o;",
                  "last_modified": "2014-11-21T14:23:02.206740",
                  "bytes": 0,
                  "name": "sym_obj",
                  "content_type": "text/plain"},
                 {"hash": "etag2",
                  "last_modified": "2014-11-21T14:14:27.409100",
                  "bytes": 32,
                  "name": "normal_obj",
                  "content_type": "text/plain"}]))
        req = Request.blank(path='/v1/a/c')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        obj_list = json.loads(body)
        self.assertIn('symlink_path', obj_list[0])
        self.assertIn(obj_list[0]['symlink_path'], '/v1/a/c/o')
        self.assertNotIn('symlink_path', obj_list[1])

    def test_get_container_with_subdir(self):
        self.app.register(
            'GET',
            '/v1/a/c?delimiter=/',
            swob.HTTPOk, {},
            json.dumps([{"subdir": "photos/"}]))
        req = Request.blank(path='/v1/a/c?delimiter=/')
        status, headers, body = self.call_sym(req)
        self.assertEqual(status, '200 OK')
        obj_list = json.loads(body)
        self.assertEqual(len(obj_list), 1)
        self.assertEqual(obj_list[0]['subdir'], 'photos/')

    def test_get_container_error_cases(self):
        # No affect for error cases
        for error in (swob.HTTPNotFound, swob.HTTPUnauthorized,
                      swob.HTTPServiceUnavailable,
                      swob.HTTPInternalServerError):
            self.app.register('GET', '/v1/a/c', error, {}, '')
            req = Request.blank(path='/v1/a/c')
            status, headers, body = self.call_sym(req)
            self.assertEqual(status, error().status)

    def test_no_affect_for_account_request(self):
        with mock.patch.object(self.sym, 'app') as mock_app:
            mock_app.return_value = (b'ok',)
            req = Request.blank(path='/v1/a')
            status, headers, body = self.call_sym(req)
            self.assertEqual(body, b'ok')

    def test_get_container_simple_with_listing_format(self):
        self.app.register(
            'GET',
            '/v1/a/c?format=json',
            swob.HTTPOk, {},
            json.dumps(
                [{"hash": "etag; symlink_target=c/o;",
                  "last_modified": "2014-11-21T14:23:02.206740",
                  "bytes": 0,
                  "name": "sym_obj",
                  "content_type": "text/plain"},
                 {"hash": "etag2",
                  "last_modified": "2014-11-21T14:14:27.409100",
                  "bytes": 32,
                  "name": "normal_obj",
                  "content_type": "text/plain"}]))
        self.lf = listing_formats.filter_factory({})(self.sym)
        req = Request.blank(path='/v1/a/c?format=json')
        status, headers, body = self.call_app(req, app=self.lf)
        self.assertEqual(status, '200 OK')
        obj_list = json.loads(body)
        self.assertIn('symlink_path', obj_list[0])
        self.assertIn(obj_list[0]['symlink_path'], '/v1/a/c/o')
        self.assertNotIn('symlink_path', obj_list[1])

    def test_get_container_simple_with_listing_format_xml(self):
        self.app.register(
            'GET',
            '/v1/a/c?format=json',
            swob.HTTPOk, {'Content-Type': 'application/json'},
            json.dumps(
                [{"hash": "etag; symlink_target=c/o;",
                  "last_modified": "2014-11-21T14:23:02.206740",
                  "bytes": 0,
                  "name": "sym_obj",
                  "content_type": "text/plain"},
                 {"hash": "etag2",
                  "last_modified": "2014-11-21T14:14:27.409100",
                  "bytes": 32,
                  "name": "normal_obj",
                  "content_type": "text/plain"}]))
        self.lf = listing_formats.filter_factory({})(self.sym)
        req = Request.blank(path='/v1/a/c?format=xml')
        status, headers, body = self.call_app(req, app=self.lf)
        self.assertEqual(status, '200 OK')
        self.assertEqual(body.split(b'\n'), [
            b'<?xml version="1.0" encoding="UTF-8"?>',
            b'<container name="c"><object><name>sym_obj</name>'
            b'<hash>etag</hash><bytes>0</bytes>'
            b'<content_type>text/plain</content_type>'
            b'<last_modified>2014-11-21T14:23:02.206740</last_modified>'
            b'</object>'
            b'<object><name>normal_obj</name><hash>etag2</hash>'
            b'<bytes>32</bytes><content_type>text/plain</content_type>'
            b'<last_modified>2014-11-21T14:14:27.409100</last_modified>'
            b'</object></container>'])
