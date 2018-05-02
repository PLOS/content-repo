#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Public Library of Science
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
Base class for CREPO Collection JSON related services
"""

import json
import logging

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from .buckets_json import DEFAULT_HEADERS
from .objects_json import OBJECTS_API

__author__ = 'gfilomeno@plos.org'

COLLECTIONS_API = API_BASE_URL + '/collections'

# Http Codes
OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404

# Error Messages
KEY_NOT_ENTERED = 'No collection key entered'
BUCKET_NOT_ENTERED = 'No bucket entered'
FILTER_NOT_ENTERED = 'At least one of the filters is required'
COLLECTION_NOT_FOUND = 'Collection not found'
BUCKET_NOT_FOUND = 'Bucket not found'
COLLECTION_NOT_EXIST = 'Can not version a collection that does not exist'


class CollectionsJson(BaseServiceTest):
    def get_collections(self, bucketName=None, **kwargs):
        """
        Calls CREPO API to get collections list in a bucket
        GET /collections?bucketName={bucketName}...

        :param bucketName, offset, limit, includeDeleted, includePurged, tag
        """
        self.doGet('%s?bucketName=%s' % (COLLECTIONS_API, bucketName), kwargs, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def post_collections(self, collection_data):
        """
        Calls CREPO API to create collection.

        :param collection_data. JSON collection structure
        """
        self.doPost('%s' % COLLECTIONS_API, data=json.dumps(collection_data), files=None,
                    headers={'Content-Type': 'application/json'})
        self.parse_response_as_json()

    def get_collection(self, bucketName=None, **kwargs):
        """
        Calls CREPO API to get a collection by key
        :param name: bucket name.key
        """
        self.doGet('%s/%s' % (COLLECTIONS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)
        self.parse_response_as_json()

    def get_collection_versions(self, bucketName=None, **kwargs):
        """
        Calls CREPO API to get a collection versions
        :param name: bucket name, key
        """
        self.doGet('%s/versions/%s' % (COLLECTIONS_API, bucketName), params=kwargs,
                   headers=DEFAULT_HEADERS)
        self.parse_response_as_json()

    def delete_collection(self, bucketName=None, **kwargs):
        """
        Calls CREPO API to delete a collection
        :param name: bucket name.
        """
        self.doDelete('%s/%s' % (COLLECTIONS_API, bucketName), params=kwargs,
                      headers=DEFAULT_HEADERS)
        if self.get_http_response().status_code != OK:
            self.parse_response_as_json()

    @needs('parsed', 'parse_response_as_json()')
    def verify_get_collections(self):
        """
        Verifies a valid response for GET /collections.
        """
        self.verify_http_code_is(OK)

    @needs('parsed', 'parse_response_as_json()')
    def verify_get_collection(self, **kwargs):
        """
        Verifies a valid response for GET /collection/{bucketName}
        """
        self.verify_http_code_is(OK)
        for k, v in kwargs.items():
            actual = self.parsed.get_collectionAttribute(k)
            self.assertEquals(actual, v, '%r is not correct: %r != %r' % (k, v, actual))

    @needs('parsed', 'parse_response_as_json()')
    def verify_get_collection_list(self, limit):
        """
        Verifies a valid response for GET /collections list
        """
        self.verify_http_code_is(OK)
        collections = self.parsed.get_collections()
        if collections:
            assert len(collections) <= 1000, \
                'Collection list returned ({0!s}) is greater than default list return set ' \
                '({1!s) size or zero'.format(len(collections), limit)
        else:
            logging.info('The collection list is empty')

    @needs('parsed', 'parse_response_as_json()')
    def verify_message_text(self, expected_message):
        assert self.parsed.get_message()[0] == expected_message, (
            'The message is not correct! actual: < {0!s}\ > expected: < {1!s} >'.format(
            self.parsed.get_message()[0].strip(), expected_message))
        logging.info(expected_message)

    def get_object_meta(self, bucketName=None, **kwargs):
        """
        Calls CREPO API to get objects list in a bucket
        GET /objects ?bucketName={bucketName}...

        :param bucketName, kwargs
        """
        self.doGet('{0!s}/meta/{1!s}'.format(OBJECTS_API, bucketName), params=kwargs,
                   headers=DEFAULT_HEADERS)
        self.parse_response_as_json()

    def post_object(self, files=None, **kwargs):
        """
        Create a new objects to build collection.
        """
        self.doPost('%s' % OBJECTS_API, data=kwargs, files=files, headers=DEFAULT_HEADERS)
        self.parse_response_as_json()

    def delete_object(self, bucketName=None, **kwargs):
        """
        Delete test objects
        """
        self.doDelete('{0!s}/{1!s}'.format(OBJECTS_API, bucketName), params=kwargs,
                      headers=DEFAULT_HEADERS)
