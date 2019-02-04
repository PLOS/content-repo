#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2019 Public Library of Science
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
POST /collections
Create a new collection. Create a new version of a collection.
Fail to create new version if collection does not exist. or bucket does not exist or object
does not exist.

GET /collections
List collections in a bucket. Limit and offset for result. Include deleted or purged.
Fail to get if bucket does not exist.

GET /collections/{bucketName} ?key
Get single collection by key or its metadata.

GET /collections/versions/{bucketName} ?key
Get all the versions of a collection.

DELETE /collections/{bucketName} ?key
Delete a collection.
"""
import io
import logging
import random
import time
import uuid

from ..api.RequestObject.collections_json import CollectionsJson, OK, CREATED, BAD_REQUEST, \
    NOT_FOUND, \
    KEY_NOT_ENTERED, BUCKET_NOT_ENTERED, COLLECTION_NOT_FOUND, BUCKET_NOT_FOUND, FILTER_NOT_ENTERED
from ..api.RequestObject.buckets_json import BucketsJson

__author__ = 'gfilomeno@plos.org'


class TestCollections(CollectionsJson):
    bucketName = BucketsJson.get_bucket_name()

    def setUp(self):
        self.already_done = 0
        self.objKey = None
        self.collKeys = []

    def tearDown(self):
        """
        Purge all objects and collections created in the test case
        """
        if self.already_done > 0:
            return
        # Delete collection's object for test
        self.delete_test_object()
        # Delete collections for test
        self.delete_test_collection()

    def test_post_collections_new(self):
        """
        Create a new collection.
        """
        logging.info('\nTesting POST /collections\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # verify the collection version number
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey())
        self.verify_http_code_is(OK)
        self.verify_get_collection(key=self.parsed.get_collectionKey()[0], versionNumber=0)

    def test_post_collections_version(self):
        """
        Create a new version of a collection.
        """
        logging.info('\nTesting POST /collections create a new version\n')
        # Create a new collection
        key = TestCollections.get_collection_key()
        self.post_collections(self.create_collection_request(key=key, create='auto'))
        self.verify_http_code_is(CREATED)
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey())
        version = self.parsed.get_collectionVersionNumber()[0]
        time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
        # Create a version collection
        self.post_collections(
                self.create_collection_request(key=key, objects=self.get_object_json(),
                                               create='version'))
        self.verify_http_code_is(CREATED)
        self.get_collection(self.bucketName, key=key)
        self.verify_get_collection(key=key, versionNumber=version + 1)
        self.get_collection_versions(self.bucketName, key=key)
        collection_list = self.parsed.get_collections()
        self.assertEquals(2, len(collection_list), 'there are not all collection versions')

    def test_post_collection_no_bucket(self):
        """
        Fail to post objects if no bucket
        """
        logging.info('\nTesting POST /collections without bucket\n')
        bucket_name = 'testbucket%d' % random.randint(1000, 1999)
        self.post_collections(self.create_collection_request(bucketName=bucket_name, create='new'))
        self.verify_http_code_is(NOT_FOUND)

    def test_post_collection_no_object(self):
        """
        Fail to post objects if no bucket
        """
        logging.info('\nTesting POST /collections without object \n')
        objects = [{'key': 'testobject', 'uuid': '604b8984-cf9f-4c3c-944e-d136d53770da'}]
        self.post_collections(self.create_collection_request(create='new', objects=objects))
        self.verify_http_code_is(NOT_FOUND)

    def test_post_collections_version_not_exist(self):
        """
        Fail to post objects version if not exist
        """
        logging.info('\nTesting POST /collections create version when collection does not exist\n')
        coll_key = TestCollections.get_collection_key()
        self.get_collection(bucketName=self.bucketName, key=coll_key)
        self.verify_http_code_is(NOT_FOUND)
        self.post_collections(self.create_collection_request(key=coll_key, create='version'))
        self.verify_http_code_is(BAD_REQUEST)

    """
    DELETE /collections
    """

    def test_delete_collection(self):
        """
        Delete a new collection
        """
        logging.info('\nTesting DELETE /collections\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        collection = self.parsed.get_json()
        # Delete the new collection
        self.delete_collection(bucketName=self.bucketName, key=collection['key'],
                               uuid=collection['uuid'])
        self.verify_http_code_is(OK)
        # Validate the new collection was deleted
        self.get_collection(bucketName=self.bucketName, key=collection['key'],
                            uuid=collection['uuid'])
        self.verify_http_code_is(NOT_FOUND)

    def test_delete_collection_without_filter(self):
        """
        Try to delete a new collection without filter value
        """
        logging.info('\nTesting DELETE /collections without filter\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        # Delete the new collection
        self.delete_collection(bucketName=self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(FILTER_NOT_ENTERED)

    def test_delete_collection_invalid_bucket(self):
        """
        Try to delete a new collection with a invalid bucket value
        """
        logging.info('\nTesting DELETE /collections invalid bucket name\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        collection = self.parsed.get_json()
        # Delete the new collection
        try:
            self.delete_collection(bucketName='@9%!#d', key=collection['key'],
                                   uuid=collection['uuid'])
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_delete_collection_invalid_key(self):
        """
        Try to delete a new collection with a invalid key value
        """
        logging.info('\nTesting DELETE /collections invalid key\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        collection = self.parsed.get_json()
        # Delete the new collection
        self.delete_collection(bucketName=self.bucketName, key='@9%!#d',
                               uuid=collection['uuid'])
        self.verify_http_code_is(NOT_FOUND)

    def test_delete_collection_invalid_params(self):
        """
        Try to delete a new collection with a invalid parameters
        """
        logging.info('\nTesting DELETE /collections invalid params\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Delete the new collection sending invalid parameters
        try:
            self.delete_collection(bucketName='@9%!#d', key='@9%!#d', uuid='@9%!#d')
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_delete_collection_only_bucket(self):
        """
        Try to delete a new collection with only the bucket
        """
        logging.info('\nTesting DELETE /collections only bucket\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        collection = self.parsed.get_json()
        # Delete the new collection
        self.delete_collection(bucketName=self.bucketName)
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(KEY_NOT_ENTERED)

    def test_delete_collection_only_key(self):
        """
        Try to delete a new collection with only the key
        """
        logging.info('\nTesting DELETE /collections only key\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        # Delete the new collection without bucket return a HTML 405 - Method Not Allowed
        try:
            self.delete_collection(bucketName='', key=self.parsed.get_collectionKey()[0])
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_delete_collection_only_filter(self):
        """
        Try to delete a new collection with only the key
        """
        logging.info('\nTesting DELETE /collections only filter\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Get collection uuid
        self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
        self.verify_http_code_is(OK)
        # Delete the new collection without bucket return a HTML 405 - Method Not Allowed
        try:
            self.delete_collection(bucketName='', uuid=self.parsed.get_collectionUUID()[0])
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_delete_collection_without_params(self):
        """
        Try to delete a new collection without parameters
        """
        logging.info('\nTesting DELETE /collections without params\n')
        # Create a new collection
        self.post_collections(self.create_collection_request(create='new'))
        self.verify_http_code_is(CREATED)
        # Delete the new collection without bucket return HTML 405 - Not method allowed
        try:
            self.delete_collection(bucketName='')
            self.fail('No JSON object could be decoded')
        except:
            pass

    """
    GET /collections/ list
    """

    def test_get_collections(self):
        """
        Calls CREPO API to GET /collections list
        """
        logging.info('\nTesting List collections (GET)\n')
        self.get_collections(self.bucketName)
        self.verify_get_collection_list(1000)

    def test_get_collections_limit_only(self):
        """
        Calls CREPO API to GET /collections list with a limit
        """
        logging.info('\nTesting List collections (GET) with limit only\n')
        limit = '%d' % random.randint(1, 1000)
        self.get_collections(self.bucketName, limit=limit)
        self.verify_get_collection_list(limit)

    def test_get_collections_without_bucket(self):
        """
        Calls CREPO API to GET /collections list without bucket name
        """
        logging.info('\nTesting List collections (GET) without bucket\n')
        self.get_collections(bucketName='')
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(BUCKET_NOT_ENTERED)

    def test_get_collections_invalid_bucket(self):
        """
        Calls CREPO API to GET /collections list with a invalid bucket
        """
        logging.info('\nTesting List collections (GET) with a invalid bucket\n')
        self.get_collections(bucketName='@9%&!d#')
        self.verify_http_code_is(NOT_FOUND)
        self.verify_message_text(BUCKET_NOT_FOUND)

    """
    GET /collections/{bucketName}
    """

    def test_get_collection(self):
        """
        Calls CREPO API to GET /collections/{bucketName}
        """
        logging.info('\nTesting GET collections/{bucketName}\n')
        collection_key = self.get_new_collection_key()
        self.get_collection(bucketName=self.bucketName, key=collection_key)
        self.verify_get_collection(key=collection_key, status='USED')

    def test_get_collection_bucket_only(self):
        """
        Get collections API call with bucket only
        """
        logging.info('\nTesting GET collections/{bucketName} with bucketName only\n')
        self.get_collection(bucketName=self.bucketName)
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(KEY_NOT_ENTERED)

    def test_get_collection_key_only(self):
        """
        Get collections API call with key only
        """
        logging.info('\nTesting GET collections/{bucketName} with key only\n')
        collection_key = self.get_new_collection_key()
        self.get_collection(bucketName='', key=collection_key)
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(BUCKET_NOT_ENTERED)

    def test_get_collection_without_parameters(self):
        """
        Calls CREPO API to get collection without parameters
        """
        logging.info('\nTesting GET collections/{bucketName} without parameters\n')
        self.get_collection(bucketName='')
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(BUCKET_NOT_ENTERED)

    def test_get_collection_invalid_key(self):
        """
        Get collections API call with a invalid key
        """
        logging.info('\nTesting GET collections/{bucketName} with a invalid key\n')
        self.get_collection(bucketName=self.bucketName, key='@9%&!d#')
        self.verify_http_code_is(NOT_FOUND)
        self.verify_message_text(COLLECTION_NOT_FOUND)

    def test_get_collection_invalid_bucket(self):
        """
        Get collections API call with a invalid bucket
        """
        logging.info('\nTesting GET collections/{bucketName} with a invalid bucket name\n')
        collection_key = self.get_new_collection_key()
        try:
            self.get_collection(bucketName='@9%&!d#', key=collection_key)
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_get_collection_invalid_parameters(self):
        """
        Get collections API call with invalid parameters
        """
        logging.info('\nTesting GET collections/{bucketName} with invalid parameters\n')
        try:
            self.get_collection(bucketName='@9%&!d#', key='@9%&!d#')
            self.fail('No JSON object could be decoded')
        except:
            pass

    """
    GET /collections/versions/{bucketName}
  
    """

    def test_get_collections_versions(self):
        """
        Get collections/versions API call
        """
        logging.info('\nTesting GET collections/versions/{bucketName}\n')
        collection_key = self.get_new_collection_key()
        self.get_collection_versions(bucketName=self.bucketName, key=collection_key)
        self.verify_get_collection()
        self.assertTrue(self.parsed.get_collections())
        if len(self.parsed.get_json()) > 0:
            x = 0
            for coll in self.parsed.get_collections():
                self.assertTrue(coll['versionNumber'] == x)
                x = x + 1

    def test_get_collections_versions_invalid_key(self):
        """
        Get collections/versions API call with a invalid key
        """
        logging.info('\nTesting GET collections/versions/{bucketName} with invalid key\n')
        self.get_collection_versions(bucketName=self.bucketName, key='@9%&!d#')
        self.verify_get_collection()
        # Verify the collection versions list is empty
        self.assertTrue(len(self.parsed.get_json()) == 0)

    def test_get_collections_versions_invalid_bucket(self):
        """
        Get collections/versions API call with a invalid bucket
        """
        logging.info('\nTesting GET collections/versions/{bucketName} with invalid bucket\n')
        collection_key = self.get_new_collection_key()
        try:
            self.get_collection_versions(bucketName='@9%&!d#', key=collection_key)
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_get_collections_versions_invalid_parameters(self):
        """
        Get collections/versions API call with a invalid parameters
        """
        logging.info('\nTesting GET collections/versions/{bucketName} with invalid parameters\n')
        try:
            self.get_collection_versions(bucketName='@9%&!d#', key='@9%&!d#')
            self.fail('No JSON object could be decoded')
        except:
            pass

    def test_get_collections_versions_key_only(self):
        """
        Get collections/versions API call with key only
        """
        logging.info('\nTesting GET collections/versions/{bucketName} only key\n')
        collection_key = self.get_new_collection_key()
        self.get_collection_versions(bucketName='', key=collection_key)
        self.verify_http_code_is(NOT_FOUND)
        self.verify_message_text(COLLECTION_NOT_FOUND)

    def test_get_collections_versions_bucket_only(self):
        """
        Get collections/versions API call with bucket only
        """
        logging.info('\nTesting GET collections/versions/{bucketName} only bucket\n')
        self.get_collection_versions(bucketName=self.bucketName)
        self.verify_http_code_is(BAD_REQUEST)
        self.verify_message_text(KEY_NOT_ENTERED)

    def test_get_collections_versions_without_parameters(self):
        """
        Get collections/versions API call without parameters
        """
        logging.info('\nTesting GET collections/versions/{bucketName} without params\n')
        try:
            self.get_collection_versions(bucketName='')
            self.fail('No JSON object could be decoded')
        except:
            pass

    def get_new_collection_key(self):
        """
        Calls CREPO API to POST a new collection
        :return New collection key
        """
        self.post_collections(self.create_collection_request(create='auto'))
        self.verify_http_code_is(CREATED)
        return self.parsed.get_collectionKey()[0]

    def create_collection_request(self, **params):
        if 'key' in params:
            key = params['key']
        else:
            key = TestCollections.get_collection_key()
        if 'bucketName' in params:
            bucket = params['bucketName']
        else:
            bucket = self.bucketName
        if 'objects' in params:
            objects = params['objects']
        else:
            objects = self.post_new_object()
        self.collKeys.append(key)
        user_metadata = TestCollections.get_usermetada(key)
        return {'bucketName': bucket, 'key': key,
                'create': params['create'], 'objects': objects, 'userMetadata': user_metadata}

    def get_object_json(self):
        # Get JSON objects
        self.get_object_meta(bucketName=self.bucketName, key=self.objKey)
        return {'key': self.objKey, 'uuid': self.parsed.get_objectAttribute('uuid')}

    def post_new_object(self):
        self.objKey = self.get_object_key()
        download = '%s.txt' % (self.objKey,)
        self.post_object(bucketName=self.bucketName, key=self.objKey,
                         contentType='text/plain', downloadName=download,
                         create='auto',
                         files=[('file', io.StringIO('test content - ' + self.objKey))])
        self.verify_http_code_is(CREATED)
        return self.get_object_json()

    def delete_test_object(self):
        if self.objKey:
            self.delete_object(bucketName=self.bucketName, key=self.objKey, version=0, purge=True)
            self.verify_http_code_is(OK)

    def verify_status_test_object(self):
        self.get_object_meta(bucketName=self.bucketName, key=self.objKey, version=0)
        logging.info('\nVerify if the object was deleted. HTTP status code: '
                     + str(self.get_http_response().status_code))
        return self.get_http_response().status_code == NOT_FOUND

    def delete_test_collection(self):
        for key in self.collKeys:
            self.get_collection_versions(bucketName=self.bucketName, key=key)
            collections = self.parsed.get_collections()
            if collections:
                for coll in collections:
                    self.delete_collection(bucketName=self.bucketName, key=coll['key'],
                                           version=coll['versionNumber'])
                    self.verify_http_code_is(OK)

    @staticmethod
    def get_collection_key():
        coll_uuid = uuid.uuid4()
        return 'testcollection{0!s}'.format(str(coll_uuid).replace('-', ''))

    @staticmethod
    def get_object_key():
        obj_uuid = uuid.uuid4()
        return 'testcollection{0!s}'.format(str(obj_uuid).replace('-', ''))

    @staticmethod
    def get_usermetada(key):
        return {'path': '/crepo/mogile/{0!r}'.format(key)}


if __name__ == '__main__':
    CollectionsJson._run_tests_randomly()
