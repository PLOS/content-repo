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

    def test_cleanup(self):
        """
        Purge all collections with key starting with testcollection
        """
        bucketName = BucketsJson.get_bucket_name()
        self.get_collections(bucketName=bucketName)
        collections = self.parsed.get_collections()
        if collections:
            for coll in collections:
                if coll['key'].startswith('testcollection'):
                    self.delete_collection(bucketName=bucketName, key=coll['key'], version=coll['versionNumber'],
                                           purge=True)

    def test_post_collections_new(self):
        """
        Create a new collection.
        """
        self.test_cleanup()
        bucketName = BucketsJson.get_bucket_name()
        key = TestCollections.get_collection_key()
        userMetadata = TestCollections.get_usermetada(key)
        objects = self.get_objects_json()
        collection_data = {'bucketName': bucketName, 'key': key,
                           'create': 'new', 'objects': objects, 'userMetadata': userMetadata}
        self.post_collections(collection_data)
        self.verify_http_code_is(CREATED)
        self.get_collection(bucketName, key=key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_collectionVersionNumber()[0]
        self.assertEquals(version, 0, 'version is not 0 for new')
        self.verify_get_collection(key=key, versionNumber=version)
        self.delete_collection(bucketName=bucketName, key=key, version=version)
        self.verify_http_code_is(OK)

    def test_post_collections_version(self):
        """
        Create a new version of a collection.
        """
        bucketName = BucketsJson.get_bucket_name()
        key = TestCollections.get_collection_key()
        userMetadata = TestCollections.get_usermetada(key)
        objects = self.get_objects_json()
        collection_data = {'bucketName': bucketName, 'key': key,
                           'create': 'auto', 'objects': objects, 'userMetadata': userMetadata}
        self.post_collections(collection_data)
        self.get_collection(bucketName, key=key)
        version = self.parsed.get_collectionVersionNumber()[0]
        time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
        collection_data = {'bucketName': bucketName, 'key': key,
                           'create': 'version', 'objects': objects, 'userMetadata': userMetadata}
        self.post_collections(collection_data)
        self.verify_http_code_is(CREATED)
        self.get_collection(bucketName, key=key)
        version_updated = self.parsed.get_collectionVersionNumber()[0]
        self.assertEquals(version + 1, version_updated, 'version is not incremented')
        self.verify_get_collection(key=key)
        self.get_collection_versions(bucketName, key=key)
        collection_list = self.parsed.get_collections()
        self.assertEquals(2, len(collection_list), 'there are not all collection versions')
        self.delete_collection(bucketName=bucketName, key=key, version=version)
        self.delete_collection(bucketName=bucketName, key=key, version=version_updated)

    def test_post_collection_no_bucket(self):
        """
        Fail to post objects if no bucket
        """
        bucketName = 'testbucket%d' % random.randint(1000, 1999)
        key = self.get_collection_key()
        objects = self.get_objects_json()
        collection_data = {'bucketName': bucketName, 'key': key,
                           'create': 'new', 'objects': objects}
        self.post_collections(collection_data)
        self.verify_http_code_is(NOT_FOUND)

    def test_post_collection_no_object(self):
        """
        Fail to post objects if no bucket
        """
        bucketName = BucketsJson.get_bucket_name()
        key = TestCollections.get_collection_key()
        objects = [{'key': 'testobject', 'uuid': '604b8984-cf9f-4c3c-944e-d136d53770da'}]
        collection_data = {'bucketName': bucketName, 'key': key,
                           'create': 'new', 'objects': objects}
        self.post_collections(collection_data)
        self.verify_http_code_is(NOT_FOUND)

    def test_post_collections_version_not_exist(self):
        """
        Fail to post objects version if not exist
        """
        bucketName = BucketsJson.get_bucket_name()
        key = TestCollections.get_collection_key()
        self.get_collection(bucketName=bucketName, key=key)
        self.verify_http_code_is(NOT_FOUND)
        objects = self.get_objects_json()
        collection_data = {'bucketName': bucketName, 'key': key,
                           'create': 'version', 'objects': objects}
        self.post_collections(collection_data)
        self.verify_http_code_is(BAD_REQUEST)

    def get_objects_json(self):
        # Create objects
        bucketName = BucketsJson.get_bucket_name()
        key = 'testobject%d' % random.randint(1000, 9999)
        download = '%s.txt' % (key,)
        self.post_objects_auto(bucketName=bucketName, key=key,
                               contentType='text/plain', downloadName=download,
                               create='new', files=[('file', StringIO.StringIO('test content'))])
        # Get JSON objects
        objects_records = []
        self.get_objects(bucketName=bucketName, offset=0, limit=10)
        objects = self.parsed.get_objects()
        for obj in objects:
            if obj['key'].startswith('testobject'):
                objects_records.append({'key': obj['key'], 'uuid': obj['uuid']})
        return objects_records

    def test_get_collections(self):
      """
      Validates the basic bare call for the collection list and
      also the function of the limit kwarg.
      """
      print('\nTesting List collections (GET)\n')
      bucketName = BucketsJson.get_bucket_name()
      self.get_collections(bucketName)
      self.verify_http_code_is(OK)
      collections = self.parsed.get_collections()
      assert(len(collections) <= 1000), 'Object list returned (%d) is greater than default list return set (%d) size or zero' % (str(len(collections)), 1000)
      limit = '%d' % random.randint(1, 1000)
      self.get_collections(bucketName, limit=limit)
      self.verify_http_code_is(OK)
      collections = self.parsed.get_collections()
      assert(str(len(collections)) <= str(limit)), 'Object list returned (%d) is greater than limit (%d) or zero' % (str(len(collections)), str(limit))
      print('\nDone\n')

    @staticmethod
    def get_collection_key():
        return 'testcollection%d' % random.randint(1000, 9999)

    @staticmethod
    def get_usermetada(key):
        return {'path': '/crepo/mogile/%r' % key}

if __name__ == '__main__':
    CollectionsJson._run_tests_randomly()