#!/usr/bin/env python2

"""
Base class for CREPO Collection JSON related services
"""

__author__ = 'gfilomeno@plos.org'

import json
from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from buckets_json import DEFAULT_HEADERS
from objects_json import OBJECTS_API

COLLECTIONS_API = API_BASE_URL + '/collections'

# Http Codes
OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404


class CollectionsJson(BaseServiceTest):

    def get_collections(self, **kwargs):
        """
        Calls CREPO API to get collections list in a bucket
        GET /collections ?bucketName={bucketName}...

        :param bucketName, offset, limit, includeDeleted, includePurged, tag
        """
        self.doGet('%s' % COLLECTIONS_API, kwargs, DEFAULT_HEADERS)
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
        self.doGet('%s/versions/%s' % (COLLECTIONS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)
        self.parse_response_as_json()

    def delete_collection(self, bucketName=None, **kwargs):
        """
        Calls CREPO API to delete a collection
        :param name: bucket name.
        """
        self.doDelete('%s/%s' % (COLLECTIONS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)

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

    def get_objects(self, **kwargs):
        """
        Calls CREPO API to get objects list in a bucket
        GET /objects ?bucketName={bucketName}...

        :param bucketName, offset, limit, includeDeleted, includePurged, tag
        """
        self.doGet('%s' % OBJECTS_API, kwargs, DEFAULT_HEADERS)
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
        self.doDelete('%s/%s' % (OBJECTS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)
