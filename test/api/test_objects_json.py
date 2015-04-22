#!/usr/bin/env python2

__author__ = 'msingh@plos.org'

"""
Test cases for Content Repo objects requests.

POST /objects
Create a new object. Create a new version of an object.
Fail to create new version if object does not exist. or bucket does not exist.

GET /objects
List objects in a bucket. Limit and offset for result. Include deleted or purged.
Fail to get if bucket does not exist.

GET /objects/{bucketName} ?key
Get single object or its metadata.

GET /objects/versions/{bucketName} ?key
Get all the versions of an object.

GET /objects/meta/{bucketName} ?key
Get info about an object and its versions.

DELETE /objects/{bucketName} ?key
Delete an object.
"""
from ..api.RequestObject.objects_json import ObjectsJson, OK, CREATED, BAD_REQUEST, NOT_FOUND
from ..api.RequestObject.buckets_json import BucketsJson
import random
import StringIO
import time


class TestObjects(ObjectsJson):

  def test_cleanup(self):
    """
    Purge all objects with key starting with testobject
    """
    bucketName = BucketsJson.get_bucket_name()
    self.get_objects(bucketName=bucketName)
    objects = self.parsed.get_objects()
    for obj in objects:
      if obj['key'].startswith('testobject'):
        self.delete_object(bucketName=bucketName, key=obj['key'], version=obj['versionNumber'], purge=True)

  def test_post_objects_new(self):
    """
    Create a new object.
    """
    self.test_cleanup()
    bucketName = BucketsJson.get_bucket_name()
    key = 'testobject%d' % random.randint(1000, 9999)
    download = '%s.txt' % (key,)
    self.post_objects(bucketName=bucketName, key=key,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName=bucketName, key=key)
    self.verify_http_code_is(OK)
    self.verify_get_object_meta(contentType='text/plain', downloadName=download)
    version = self.parsed.get_objectVersionNumber()[0]
    self.assertEquals(version, 0, 'version is not 0 for new')
    self.get_object(bucketName=bucketName, key=key)
    self.verify_http_code_is(OK)
    self.verify_get_object(content='test content')
    self.delete_object(bucketName=bucketName, key=key, version=version, purge=True)
    self.verify_http_code_is(OK)

  def test_post_objects_version(self):
    """
    Create a new version of an object.
    """
    bucketName = BucketsJson.get_bucket_name()
    key = 'testobject%d' % random.randint(1000, 9999)
    download = '%s.txt' % (key,)
    self.post_objects(bucketName=bucketName, key=key,
                      contentType='text/plain', downloadName=download,
                      create='auto', files=[('file', StringIO.StringIO('test content'))])
    self.get_object_meta(bucketName=bucketName, key=key)
    version = self.parsed.get_objectVersionNumber()[0]
    self.get_object(bucketName=bucketName, key=key)
    time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
    download_updated = '%supdated.txt' % (key,)
    self.post_objects(bucketName=bucketName, key=key,
                      contentType='text/plain', downloadName=download_updated,
                      create='version', files=[('file', StringIO.StringIO('test content updated'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName=bucketName, key=key)
    self.verify_http_code_is(OK)
    self.verify_get_object_meta(contentType='text/plain', downloadName=download_updated)
    version_updated = self.parsed.get_objectVersionNumber()[0]
    self.get_object(bucketName=bucketName, key=key)
    self.verify_http_code_is(OK)
    self.verify_get_object(content='test content updated')
    self.assertEquals(version + 1, version_updated, 'version is not incremented')
    self.delete_object(bucketName=bucketName, key=key, version=version, purge=True)
    self.delete_object(bucketName=bucketName, key=key, version=version_updated, purge=True)

  def test_post_objects_no_bucket(self):
    """
    Fail to post objects if no bucket
    """
    bucketName = 'testbucket%d' % random.randint(1000, 1999)
    key = 'testobject%d' % random.randint(1000, 9999)
    download = '%s.txt' % (key,)
    self.post_objects(bucketName=bucketName, key=key,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(NOT_FOUND)

  def test_post_objects_version_not_exist(self):
    """
    Fail to post objects version if not exist
    """
    bucketName = BucketsJson.get_bucket_name()
    key = 'testobject%d' % random.randint(1000, 9999)
    self.get_object(bucketName=bucketName, key=key)
    self.verify_http_code_is(NOT_FOUND)
    download = '%s-updated.txt' % (key,)
    self.post_objects(bucketName=bucketName, key=key,
                      contentType='text/plain', downloadName=download,
                      create='version', files=[('file', StringIO.StringIO('test content updated'))])
    self.verify_http_code_is(BAD_REQUEST)


if __name__ == '__main__':
    ObjectsJson._run_tests_randomly()
