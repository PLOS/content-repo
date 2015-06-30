#!/usr/bin/env python2

__author__ = 'msingh@plos.org'

"""
Test cases for Content Repo objects requests.

POST /objects
Create a new object. Create a new version of an object.
Fail to create new version if object does not exist. or bucket does not exist.

GET /objects
List objects in a bucket. Limit and offset for result. Include deleted or purged.
Get list using tag.
Fail to get if bucket does not exist.

GET /objects/{bucketName} ?key
Get single object or its metadata.

GET /objects/versions/{bucketName} ?key
Get all the versions of an object.

GET /objects/meta/{bucketName} ?key,version
Get info about an object and its versions. Get by specific version.
Fail to get if not found.

DELETE /objects/{bucketName} ?key,version,purge
Delete an object. With or without version or purge parameters.
Fail to delete if not found. Delete last version, to make previous visible.
"""

from ..api.RequestObject.objects_json import ObjectsJson, OK, CREATED, BAD_REQUEST, NOT_FOUND, NOT_ALLOWED
from ..api.RequestObject.buckets_json import BucketsJson
import random
import StringIO
import time


class TestObjects(ObjectsJson):

  objKey = None

  def setUp(self):
    self.objKey = None
    self.already_done = 0


  def tearDown(self):
    """
    Purge all objects created in the test case
    """
    if self.already_done > 0: return
    objects = self.get_objects_json()
    if objects:
      for obj in objects:
        self.delete_object(bucketName=BucketsJson.get_bucket_name(), key=obj['key'], uuid=obj['uuid'], purge=True)
        self.verify_http_code_is(OK)

  def test_post_objects_new(self):
    """
    Create a new object.
    """
    print 'Tests POST /objects new'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    self.verify_get_object_meta(contentType='text/plain', downloadName=download)
    self.get_object(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    self.verify_get_object(content='test content')

  def test_post_objects_version(self):
    """
    Create a new version of an object.
    """
    print 'Tests POST /objects version'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='auto', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
    download_updated = '%supdated.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download_updated,
                      create='version', files=[('file', StringIO.StringIO('test content updated'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    self.verify_get_object_meta(contentType='text/plain', downloadName=download_updated)
    version_updated = self.parsed.get_objectVersionNumber()[0]
    self.get_object(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    self.verify_get_object(content='test content updated')
    self.assertEquals(version + 1, version_updated, 'version is not incremented')

  def test_post_objects_no_bucket(self):
    """
    Fail to post objects if no bucket
    """
    print 'Tests POST /objects not bucket'
    bucketName = 'testbucket%d' % random.randint(1000, 1999)
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(NOT_FOUND)

  def test_post_objects_version_not_exist(self):
    """
    Fail to post objects version if not exist
    """
    print 'Tests POST /objects version does not exist'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    self.get_object(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)
    download = '%s-updated.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='version', files=[('file', StringIO.StringIO('test content updated'))])
    self.verify_http_code_is(BAD_REQUEST)

  def test_get_objects_list(self):
    """
    Get list of objects
    """
    print 'Tests GET /objects list'
    bucketName = BucketsJson.get_bucket_name()
    self.get_objects(bucketName)
    self.verify_http_code_is(OK)
    objects = self.parsed.get_objects()
    self.assertTrue((objects and len(objects) or 0) <= 1000,
        'more than 1000 items: %r'%(objects and len(objects),))

  def test_get_objects_limit(self):
    """
    Get list of objects with limit
    """
    print 'Tests GET /objects with limit'
    bucketName = BucketsJson.get_bucket_name()
    self.get_objects(bucketName)
    self.verify_http_code_is(OK)
    objects_all = self.parsed.get_objects()
    length = len(objects_all)
    limit = random.randint(1, length)
    self.get_objects(bucketName, limit=limit)
    self.verify_http_code_is(OK)
    objects = self.parsed.get_objects()
    self.assertEquals(objects and len(objects), limit,
        'incorrect items count with limit=%d: %r != %r'%(limit, objects and len(objects), limit))
    for left, right in zip(objects_all[:limit], objects):
      self.assertEquals(left, right, 'non-matching item %r != %r'%(left, right))

  def test_get_objects_offset(self):
    """
    Get list of objects with offset and limit
    """
    print 'Tests GET /objects with offset'
    bucketName = BucketsJson.get_bucket_name()
    self.get_objects(bucketName)
    self.verify_http_code_is(OK)
    objects_all = self.parsed.get_objects()
    length = len(objects_all)
    offset = random.randint(1, length)
    self.get_objects(bucketName, offset=offset, limit=length-offset)
    self.verify_http_code_is(OK)
    objects = self.parsed.get_objects()
    self.assertEquals(objects and len(objects), length - offset,
        'incorrect items count with offset=%d: %r != %r'%(offset, objects and len(objects), length - offset))
    for left, right in zip(objects_all[offset:], objects):
      self.assertEquals(left, right, 'non-matching item %r != %r'%(left, right))

  def test_get_objects_by_tag(self):
    """
    Get list of objects by tag
    """
    print 'Tests GET /objects with tag'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    tag = 'tag-' + self.objKey
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download, tag=tag,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_objects(bucketName, tag=tag)
    self.verify_http_code_is(OK)
    objects = self.parsed.get_objects()
    self.assertEquals(objects and len(objects), 1, 'failed with tag=%r'%(tag,))

  def test_get_objects_deleted(self):
    """
    Get deleted list of objects.
    """
    print 'Tests GET /objects list deleted'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    tag = 'tag-' + self.objKey
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download, tag=tag,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_objects(bucketName, tag=tag)
    self.verify_http_code_is(OK)
    objects0 = self.parsed.get_objects()

    self.delete_object(bucketName=bucketName, key=self.objKey, version=objects0[0]['versionNumber'])
    self.verify_http_code_is(OK)
    self.get_objects(bucketName, tag=tag)
    self.verify_http_code_is(OK)
    self.assertEquals(self.parsed.get_objects(), False, 'failed with tag=%r, after deleting'%(tag,))
    self.get_objects(bucketName, tag=tag, includeDeleted=True)
    self.verify_http_code_is(OK)
    objects1 = self.parsed.get_objects()
    self.assertEquals(objects1 and len(objects1), 1,
                      'failed with tag=%r includeDeleted=true, after deleting'%(tag,))
    self.assertEquals(objects1[0]['status'], 'DELETED', 'wrong status after deleting: %r'%(objects1[0]['status'],))
    for key in objects0[0].keys():
      if key != "status":
        self.assertEquals(objects0[0][key], objects1[0][key], 'wrong attribute %r: %r != %r'%(key, objects0[0][key], objects1[0][key]))

  def test_get_objects_purged(self):
    """
    Get purged list of objects.
    """
    print 'Tests GET /objects list purged'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    tag = 'tag-' + self.objKey
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download, tag=tag,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_objects(bucketName, tag=tag)
    self.verify_http_code_is(OK)
    objects0 = self.parsed.get_objects()
    self.assertEquals(objects0 and len(objects0), 1, 'failed with tag=%r'%(tag,))

    self.delete_object(bucketName=bucketName, key=self.objKey, version=objects0[0]['versionNumber'], purge=True)
    self.verify_http_code_is(OK)
    self.get_objects(bucketName, tag=tag)
    self.verify_http_code_is(OK)
    self.assertEquals(self.parsed.get_objects(), False, 'failed with tag=%r, after purging'%(tag,))
    self.get_objects(bucketName, tag=tag, includeDeleted=True)
    self.verify_http_code_is(OK)
    self.assertEquals(self.parsed.get_objects(), False, 'failed with tag=%r includeDeleted=True, after purging'%(tag,))
    self.get_objects(bucketName, tag=tag, includePurged=True)
    self.verify_http_code_is(OK)
    objects1 = self.parsed.get_objects()
    self.assertTrue(objects1 and len(objects1) >= 1,
                      'failed with tag=%r includePurged=true, after purging'%(tag,))
    self.assertEquals(objects1[0]['status'], 'PURGED', 'wrong status after purged: %r'%(objects1[0]['status'],))
    for key in objects0[0].keys():
      if key != "status" and key != "timestamp":
        self.assertEquals(objects0[0][key], objects1[0][key], 'wrong attribute %r: %r != %r'%(key, objects0[0][key], objects1[0][key]))

  def test_get_objects_invalid_bucket(self):
    """
    Failed to list object if bucket not found.
    """
    print 'Tests GET /objects invalid bucket'
    bucketName = 'bucket%d' % self.next_random()
    self.get_objects(bucketName)
    self.verify_http_code_is(NOT_FOUND)

  def test_get_objects_no_bucket(self):
    """
    Failed to list object if no bucketname supplied.
    """
    print 'Tests GET /objects not bucket'
    self.get_objects()
    self.verify_http_code_is(BAD_REQUEST)

  def test_get_objects_meta(self):
    """
    Get object metadata
    """
    print 'Tests GET /objects metadata'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.verify_get_object_meta(contentType='text/plain', downloadName=download, versionNumber=version)

  def test_get_objects_meta_version(self):
    """
    Get object metadata for specific version
    """
    print 'Tests GET /objects metadata version'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)

    time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName="updated" + download,
                      create='version', files=[('file', StringIO.StringIO('test content updated'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.verify_get_object_meta(contentType='text/plain', downloadName="updated" + download, versionNumber=version)
    self.get_object_meta(bucketName, key=self.objKey, version=version-1)
    self.verify_http_code_is(OK)
    self.verify_get_object_meta(contentType='text/plain', downloadName=download, versionNumber=version-1)

  def test_get_objects_meta_not_found(self):
    """
    Fail to get metadata if bucket or object not found.
    """
    print 'Tests GET /objects meta not found'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)
    bucketName = 'testbucket%d' % self.next_random()
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)

  def test_get_objects_meta_no_bucket_no_key(self):
    """
    Fail to get object metadata without bucketName, or without key, or without both.
    """
    print 'Tests GET /objects meta not bucket not key'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)

    self.get_object_meta(key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)

    self.get_object_meta(bucketName)
    self.verify_http_code_is(NOT_FOUND)

    self.get_object_meta()
    self.verify_http_code_is(NOT_FOUND)

    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.verify_get_object_meta(contentType='text/plain', downloadName=download, versionNumber=version)

  def test_delete_objects_with_version(self):
    print 'Tests DELETE /objects with version'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.delete_object(bucketName=bucketName, key=self.objKey, version=version, purge=True)
    self.verify_http_code_is(OK)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)

  def test_delete_objects_with_tag(self):
    print 'Tests DELETE /objects with tag'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    tag = 'tag-' + self.objKey
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download, tag=tag,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    self.delete_object(bucketName=bucketName, key=self.objKey, tag=tag, purge=True)
    self.verify_http_code_is(OK)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)

  def test_delete_objects_with_uuid(self):
    print 'Tests DELETE /objects with uuid'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    uuid = self.parsed.get_objectAttribute("uuid");
    self.delete_object(bucketName=bucketName, key=self.objKey, uuid=uuid, purge=True)
    self.verify_http_code_is(OK)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)

  def test_delete_objects_only_key(self):
    print 'Tests DELETE /objects only key'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    version = self.parsed.get_objectVersionNumber()[0]
    self.verify_http_code_is(OK)
    self.delete_object(bucketName=bucketName, key=self.objKey, purge=True)
    self.verify_http_code_is(BAD_REQUEST)

  def test_delete_objects_not_found(self):
    print 'Tests DELETE /objects invalid key'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(NOT_FOUND)

  def test_delete_objects_last_version(self):
    print 'Tests DELETE /objects last version'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    time.sleep(1)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName="updated" + download,
                      create='version', files=[('file', StringIO.StringIO('test content updated'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.delete_object(bucketName=bucketName, key=self.objKey, version=version, purge=True)
    self.verify_http_code_is(OK)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    self.verify_get_object_meta(versionNumber=version-1)

  def test_delete_objects_purge_or_not(self):
    print 'Tests DELETE /objects purge or not'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    tag = 'tag-' + self.objKey
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download, tag=tag,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.delete_object(bucketName=bucketName, key=self.objKey, version=version, purge=True)
    self.verify_http_code_is(OK)
    self.get_object_meta(bucketName, key=self.objKey, version=version)
    self.verify_http_code_is(NOT_FOUND)
    self.get_objects(bucketName, tag=tag, includePurged=True)
    self.verify_http_code_is(OK)
    objects = self.parsed.get_objects()
    self.assertEquals(objects and len(objects), 1, 'failed to get purged objects')
    self.assertEquals(objects[0]['status'], 'PURGED', 'status is not correct after purge: %r'%(objects[0]['status'],))

    key = 'testobject%d' % self.next_random()
    download = '%s.txt' % (key,)
    tag = 'tag-' + key
    self.post_objects(bucketName=bucketName, key=key,
                      contentType='text/plain', downloadName=download, tag=tag,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=key)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]
    self.delete_object(bucketName=bucketName, key=key, version=version, purge=False)
    self.verify_http_code_is(OK)
    self.get_object_meta(bucketName, key=key, version=version)
    self.verify_http_code_is(NOT_FOUND)
    self.get_objects(bucketName, tag=tag, includeDeleted=True)
    self.verify_http_code_is(OK)
    objects = self.parsed.get_objects()
    self.assertEquals(objects and len(objects), 1, 'failed to get deleted objects')
    self.assertEquals(objects[0]['status'], 'DELETED', 'status is not correct after delete: %r'%(objects[0]['status'],))

  def test_delete_objects_no_bucket_no_key(self):
    """
    Fail to delete object with invalid bucketName or key, or empty, or for both.
    """
    print 'Tests DELETE /objects not bucket not key'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version = self.parsed.get_objectVersionNumber()[0]

    # invalid bucket, valid key
    bucketName1 = 'testbucket%d' % self.next_random()
    self.delete_object(bucketName=bucketName1, key=self.objKey, version=version, purge=True)
    self.verify_http_code_is(NOT_FOUND)

    # valid bucket, invalid key
    key1 = 'testobject%d' % self.next_random()
    self.delete_object(bucketName=bucketName, key=key1, version=version, purge=True)
    self.verify_http_code_is(NOT_FOUND)

    # invalid bucket, invalid key
    self.delete_object(bucketName=bucketName1, key=key1, version=version, purge=True)
    self.verify_http_code_is(NOT_FOUND)

    # valid bucket, no key
    self.delete_object(bucketName=bucketName, version=version, purge=True)
    self.verify_http_code_is(BAD_REQUEST)

    # no bucket, valid key
    self.delete_object(key=self.objKey, version=version, purge=True)
    self.verify_http_code_is(NOT_ALLOWED)

    # no bucket, no key
    self.delete_object(version=version, purge=True)
    self.verify_http_code_is(NOT_ALLOWED)

  def test_get_objects_versions(self):
    print 'Tests GET /objects versions'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version0 = self.parsed.get_objectVersionNumber()[0]
    time.sleep(1)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName="updated" + download,
                      create='version', files=[('file', StringIO.StringIO('updated test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    version1 = self.parsed.get_objectVersionNumber()[0]

    self.get_object_versions(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), 2, "incorrect number of versions: %r != 2"%(versions and len(versions),))
    self.assertEquals(versions[0], version0, "incorrect first version: %r != %r"%(versions[0], version0))
    self.assertEquals(versions[1], version1, "incorrect first version: %r != %r"%(versions[1], version1))
    self.delete_object(bucketName, key=self.objKey, version=version0, purge=True)
    self.verify_http_code_is(OK)

    self.get_object_versions(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), 1, "incorrect number of versions after one delete: %r != 1"%(versions and len(versions),))
    self.assertEquals(versions[0], version1, "incorrect first version after one delete: %r != %r"%(versions[0], version1))

    self.delete_object(bucketName, key=self.objKey, version=version1, purge=True)
    self.verify_http_code_is(OK)

    self.get_object_versions(bucketName=bucketName, key=self.objKey)
    self.verify_http_code_is(OK)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False, "incorrect number of versions after both delete: %r != False"%(versions and len(versions),))


  def test_get_objects_versions_not_found(self):
    """
    Fail to get objects versions if bucketName or key are invalid or empty, or for both.
    """
    print 'Tests GET /objects versions invalid bucket'
    bucketName = BucketsJson.get_bucket_name()
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_objects(bucketName=bucketName, key=self.objKey,
                      contentType='text/plain', downloadName=download,
                      create='new', files=[('file', StringIO.StringIO('test content'))])
    self.verify_http_code_is(CREATED)
    self.get_object_meta(bucketName, key=self.objKey)
    self.verify_http_code_is(OK)

    # valid bucket, invalid key
    key1 = 'testobject%d' % self.next_random()
    self.get_object_versions(bucketName=bucketName, key=key1)
    self.verify_http_code_is(OK)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False,
                      "valid bucket, invalid key - incorrect number of versions: %r != False"%(versions and len(versions),))

    # invalid bucket, valid key
    bucketName1 = 'testbucket%d' % self.next_random()
    self.get_object_versions(bucketName=bucketName1, key=self.objKey)
    self.verify_http_code_is(OK)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False,
                      "invalid bucket, valid key - incorrect number of versions: %r != False"%(versions and len(versions),))

    # invalid bucket, invalid key
    self.get_object_versions(bucketName=bucketName1, key=key1)
    self.verify_http_code_is(OK)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False,
                      "invalid bucket, invalid key - incorrect number of versions: %r != False"%(versions and len(versions),))

    # no bucket, valid key
    self.get_object_versions(key=self.objKey)
    self.verify_http_code_is(BAD_REQUEST)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False,
                      "no bucket, valid key - incorrect number of versions: %r != False"%(versions and len(versions),))

    # valid bucket, no key
    self.get_object_versions(bucketName=bucketName,)
    self.verify_http_code_is(BAD_REQUEST)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False,
                      "valid bucket, no key - incorrect number of versions: %r != False"%(versions and len(versions),))

    # no bucket, no key
    self.get_object_versions()
    self.verify_http_code_is(BAD_REQUEST)
    versions = self.parsed.get_objectVersionNumber()
    self.assertEquals(versions and len(versions), False,
                      "no bucket, no key - incorrect number of versions: %r != False"%(versions and len(versions),))


  def get_objects_json(self):
    # Get JSON objects
    objects_records = []
    if self.objKey:
      self.get_object_versions(bucketName=BucketsJson.get_bucket_name(), key=self.objKey)
      objects = self.parsed.get_objects()
      if objects:
        for obj in objects:
          objects_records.append({'key': obj['key'], 'uuid': obj['uuid']})
    return objects_records

  @staticmethod
  def get_object_key():
    return 'testobject%d' % TestObjects.next_random()

  @staticmethod
  def next_random():
    return random.randint(1000, 9999)

if __name__ == '__main__':
    ObjectsJson._run_tests_randomly()
