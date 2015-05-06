#!/usr/bin/env python2

__author__ = 'gfilomeno@plos.org'

"""
POST /collections
Create a new collection. Create a new version of a collection.
Fail to create new version if collection does not exist. or bucket does not exist or object does not exist.

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
from ..api.RequestObject.collections_json import CollectionsJson, OK, CREATED, BAD_REQUEST, NOT_FOUND
from ..api.RequestObject.buckets_json import BucketsJson
import random
import StringIO
import time


class TestCollections(CollectionsJson):

    bucketName = BucketsJson.get_bucket_name()

    def test_cleanup(self):
        """
        Purge all objects and collections with key starting with testobject/testcollection
        """
        objects = self.get_objects_json()
        if objects:
            for obj in objects:
                if obj['key'].startswith('testobject'):
                    self.delete_object(bucketName=self.bucketName, key=obj['key'], uuid=obj['uuid'], purge=True)

        self.get_collections(bucketName=self.bucketName)
        collections = self.parsed.get_collections()
        if collections:
            for coll in collections:
                if coll['key'].startswith('testcollection'):
                    self.delete_collection(bucketName=self.bucketName, key=coll['key'], version=coll['versionNumber'], purge=True)

    def test_post_collections_new(self):
        """
        Create a new collection.
        """

        key = TestCollections.get_collection_key()
        user_metadata = TestCollections.get_usermetada(key)
        # Obtain test objects for collection
        objects = self.post_objects()
        collection_data = {'bucketName': self.bucketName, 'key': key,
                             'create': 'new', 'objects': objects, 'userMetadata': user_metadata}
        # Create a new collection
        self.post_collections(collection_data)
        self.verify_http_code_is(CREATED)
        # verify the collection version number
        self.get_collection(self.bucketName, key=key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_collectionVersionNumber()[0]
        self.assertEquals(version, 0, 'version is not 0 for new')
        self.verify_get_collection(key=key, versionNumber=version)
        # Delete the new collection and objects
        self.test_cleanup()

    def test_post_collections_version(self):
        """
        Create a new version of a collection.
        """
        key = TestCollections.get_collection_key()
        user_metadata = TestCollections.get_usermetada(key)
        objects = self.post_objects()
        collection_data = {'bucketName': self.bucketName, 'key': key,
                           'create': 'auto', 'objects': objects, 'userMetadata': user_metadata}
        # Create a new collection
        self.post_collections(collection_data)
        self.get_collection(self.bucketName, key=key)
        version = self.parsed.get_collectionVersionNumber()[0]
        time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
        collection_data = {'bucketName': self.bucketName, 'key': key,
                           'create': 'version', 'objects': objects, 'userMetadata': user_metadata}
        # Create a version collection
        self.post_collections(collection_data)
        self.verify_http_code_is(CREATED)
        self.get_collection(self.bucketName, key=key)
        version_updated = self.parsed.get_collectionVersionNumber()[0]
        # validate version number collection
        self.assertEquals(version + 1, version_updated, 'version is not incremented')
        self.verify_get_collection(key=key)
        self.get_collection_versions(self.bucketName, key=key)
        collection_list = self.parsed.get_collections()
        self.assertEquals(2, len(collection_list), 'there are not all collection versions')
        # Delete objects and collections
        self.test_cleanup()

    def test_post_collection_no_bucket(self):
        """
        Fail to post objects if no bucket
        """
        bucket_name = 'testbucket%d' % random.randint(1000, 1999)
        key = self.get_collection_key()
        objects = self.post_objects()
        collection_data = {'bucketName': bucket_name, 'key': key,
                           'create': 'new', 'objects': objects}
        self.post_collections(collection_data)
        self.verify_http_code_is(NOT_FOUND)

    def test_post_collection_no_object(self):
        """
        Fail to post objects if no bucket
        """
        key = TestCollections.get_collection_key()
        objects = [{'key': 'testobject', 'uuid': '604b8984-cf9f-4c3c-944e-d136d53770da'}]
        collection_data = {'bucketName': self.bucketName, 'key': key,
                           'create': 'new', 'objects': objects}
        self.post_collections(collection_data)
        self.verify_http_code_is(NOT_FOUND)

    def test_post_collections_version_not_exist(self):
        """
        Fail to post objects version if not exist
        """
        key = TestCollections.get_collection_key()
        self.get_collection(bucketName=self.bucketName, key=key)
        self.verify_http_code_is(NOT_FOUND)
        objects = self.post_objects()
        collection_data = {'bucketName': self.bucketName, 'key': key,
                           'create': 'version', 'objects': objects}
        self.post_collections(collection_data)
        self.verify_http_code_is(BAD_REQUEST)
        # Delete objects created
        self.test_cleanup()

    def get_objects_json(self):
        # Get JSON objects
        objects_records = []
        self.get_objects(bucketName=self.bucketName)
        objects = self.parsed.get_objects()
        if objects:
            for obj in objects:
                if obj['key'].startswith('testobject'):
                    objects_records.append({'key': obj['key'], 'uuid': obj['uuid']})
        return objects_records

    def post_objects(self):
        objects_records = self.get_objects_json()
        if not objects_records:
            key = 'testobject%d' % random.randint(1000, 9999)
            download = '%s.txt' % (key,)
            self.post_object(bucketName=self.bucketName, key=key,
                                   contentType='text/plain', downloadName=download,
                                  create='auto', files=[('file', StringIO.StringIO('test content'))])
            objects_records = self.get_objects_json()
        return objects_records



    @staticmethod
    def get_collection_key():
        return 'testcollection%d' % random.randint(1000, 9999)

    @staticmethod
    def get_usermetada(key):
        return {'path': '/crepo/mogile/%r' % key}


if __name__ == '__main__':
    CollectionsJson._run_tests_randomly()
