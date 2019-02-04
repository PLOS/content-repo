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
Test cases for Content Repo objects requests.

POST /objects
Create a new object. Create a new version of an object.
Fail to create new version if object does not exist. or bucket does not exist.

GET /objects
List objects in a bucket. Limit and offset for result. Include deleted or purged.
Get list using tag.
Fail to get if bucket does not exist.

GET /objects/{bucket_name} ?key
Get single object or its metadata.

GET /objects/versions/{bucket_name} ?key
Get all the versions of an object.

GET /objects/meta/{bucket_name} ?key,version
Get info about an object and its versions. Get by specific version.
Fail to get if not found.

DELETE /objects/{bucket_name} ?key,version,purge
Delete an object. With or without version or purge parameters.
Fail to delete if not found. Delete last version, to make previous visible.
"""

from io import BytesIO, StringIO
import logging
import time
from random import randint

from ..api.RequestObject.objects_json import ObjectsJson, OK, CREATED, BAD_REQUEST, NOT_FOUND, \
    NOT_ALLOWED
from ..api.RequestObject.buckets_json import BucketsJson

__author__ = 'msingh@plos.org'


class TestObjects(ObjectsJson):
    obj_key = None

    def setUp(self):
        self.obj_key = None
        self.already_done = 0

    def tearDown(self):
        """
        Purge all objects created in the test case
        """
        if self.already_done > 0: 
            return
        objects = self.get_objects_json()
        if objects:
            for obj in objects:
                self.delete_object(bucketName=BucketsJson.get_bucket_name(), key=obj['key'],
                                   uuid=obj['uuid'], purge=True)
                self.verify_http_code_is(OK)

    def test_post_objects_new(self):
        """
        Create a new object.
        """
        logging.info('Tests POST /objects new')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        self.verify_get_object_meta(contentType='text/plain', downloadName=download)
        self.get_object(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        self.verify_get_object(content='test content')

    def test_post_objects_version(self):
        """
        Create a new version of an object.
        """
        logging.info('Tests POST /objects version')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download,
                          create='auto', files=[('file', BytesIO(b'test content'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
        download_updated = '{0!s}updated.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download_updated,
                          create='version', files=[('file', BytesIO(b'test content updated'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        self.verify_get_object_meta(contentType='text/plain', downloadName=download_updated)
        version_updated = self.parsed.get_objectVersionNumber()[0]
        self.get_object(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        self.verify_get_object(content='test content updated')
        self.assertEquals(version + 1, version_updated, 'version is not incremented')

    def test_post_objects_no_bucket(self):
        """
        Fail to post objects if no bucket
        """
        logging.info('Tests POST /objects not bucket')
        bucket_name = 'testbucket{0!s}'.format(self.create_key())
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download,
                          create='new', files=[('file', BytesIO(b'test content'))])
        self.verify_http_code_is(NOT_FOUND)

    def test_post_objects_version_not_exist(self):
        """
        Fail to post objects version if not exist
        """
        logging.info('Tests POST /objects version does not exist')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        self.get_object(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)
        download = '{0!s}-updated.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download,
                          create='version', files=[('file', BytesIO(b'test content updated'))])
        self.verify_http_code_is(BAD_REQUEST)

    def test_get_objects_list(self):
        """
        Get list of objects
        """
        logging.info('Tests GET /objects list')
        bucket_name = BucketsJson.get_bucket_name()
        self.get_objects(bucket_name)
        self.verify_http_code_is(OK)
        objects = self.parsed.get_objects()
        assert (objects and len(objects) or 0) <= 1000, 'more than 1000 items: {0!r}'.format(
                objects and len(objects))

    def test_get_objects_limit(self):
        """
        Get list of objects with limit
        """
        logging.info('Tests GET /objects with limit')
        bucket_name = BucketsJson.get_bucket_name()
        self.get_objects(bucket_name)
        self.verify_http_code_is(OK)
        objects_all = self.parsed.get_objects()
        length = len(objects_all)
        limit = randint(1, length)
        self.get_objects(bucket_name, limit=limit)
        self.verify_http_code_is(OK)
        objects = self.parsed.get_objects()
        self.assertEquals(objects and len(objects), limit,
                          'incorrect items count with limit=%d: %r != %r' % (
                              limit, objects and len(objects), limit))
        for left, right in zip(objects_all[:limit], objects):
            for key in left.keys():
                if key != "status" and key != "timestamp":
                    if key == "reproxyURL":
                        self.assertEquals(set(left[key]), set(right[key]),
                                          'wrong attribute %r: %r != %r' % (
                                              key, left[key], right[key]))
                    else:
                        self.assertEquals(left[key], right[key],
                                          'wrong attribute %r: %r != %r' % (
                                              key, left[key], right[key]))

    def test_get_objects_offset(self):
        """
        Get list of objects with offset and limit
        """
        logging.info('Tests GET /objects with offset')
        bucket_name = BucketsJson.get_bucket_name()
        self.get_objects(bucket_name)
        self.verify_http_code_is(OK)
        objects_all = self.parsed.get_objects()
        length = len(objects_all)
        offset = randint(1, length)
        self.get_objects(bucket_name, offset=offset, limit=length - offset)
        self.verify_http_code_is(OK)
        objects = self.parsed.get_objects()
        self.assertEquals(objects and len(objects), length - offset,
                          'incorrect items count with offset=%d: %r != %r' % (
                              offset, objects and len(objects), length - offset))
        for left, right in zip(objects_all[offset:], objects):
            for key in left.keys():
                if key != "status" and key != "timestamp":
                    if key == "reproxyURL":
                        self.assertEquals(set(left[key]), set(right[key]),
                                          'wrong attribute %r: %r != %r' % (
                                              key, left[key], right[key]))
                    else:
                        self.assertEquals(left[key], right[key],
                                          'wrong attribute %r: %r != %r' % (
                                              key, left[key], right[key]))

    def test_get_objects_by_tag(self):
        """
        Get list of objects by tag
        """
        logging.info('Tests GET /objects with tag')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        tag = 'tag-' + self.obj_key
        download = '{0!s}.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download, tag=tag,
                          create='new', files=[('file', StringIO('test content')), ])
        self.verify_http_code_is(CREATED)
        self.get_objects(bucket_name, tag=tag)
        self.verify_http_code_is(OK)
        objects = self.parsed.get_objects()
        self.assertEquals(objects and len(objects), 1, 'failed with tag=%r' % (tag,))

    def test_get_objects_deleted(self):
        """
        Get deleted list of objects.
        """
        logging.info('Tests GET /objects list deleted')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        tag = 'tag-' + self.obj_key
        download = '{0!s}.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download, tag=tag,
                          create='new', files=[('file', BytesIO(b'test content'))])
        self.verify_http_code_is(CREATED)
        self.get_objects(bucket_name, tag=tag)
        self.verify_http_code_is(OK)
        objects0 = self.parsed.get_objects()

        self.delete_object(bucketName=bucket_name, key=self.obj_key,
                           version=objects0[0]['versionNumber'])
        self.verify_http_code_is(OK)
        self.get_objects(bucket_name, tag=tag)
        self.verify_http_code_is(OK)
        self.assertEquals(self.parsed.get_objects(), False,
                          'failed with tag=%r, after deleting' % (tag,))
        self.get_objects(bucket_name, tag=tag, includeDeleted=True)
        self.verify_http_code_is(OK)
        objects1 = self.parsed.get_objects()
        self.assertTrue(objects1 and len(objects1) >= 1,
                        'failed with tag=%r includeDeleted=true, after deleting: %r' % (
                            tag, objects1 and len(objects1)))
        self.assertEquals(objects1[-1]['status'], 'DELETED',
                          'wrong status after deleting: %r' % (objects1[-1]['status'],))
        for key in objects0[0].keys():
            if key != "status" and key != "timestamp":
                if key == "reproxyURL":
                    self.assertEquals(set(objects0[0][key]), set(objects1[-1][key]),
                                      'wrong attribute %r: %r != %r' % (
                                          key, objects0[0][key], objects1[-1][key]))
                else:
                    self.assertEquals(objects0[0][key], objects1[-1][key],
                                      'wrong attribute %r: %r != %r' % (
                                          key, objects0[0][key], objects1[-1][key]))

    def test_get_objects_purged(self):
        """
        Get purged list of objects.
        """
        logging.info('Tests GET /objects list purged')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        tag = 'tag-' + self.obj_key
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download, tag=tag,
                              create='new', files=[('file', f), ])
        # self.post_objects(bucketName=bucket_name, key=self.obj_key,
        #                   contentType='text/plain', downloadName=download, tag=tag,
        #                   create='new', files=[('file', BytesIO(b'test content'))])
        self.verify_http_code_is(CREATED)
        self.get_objects(bucket_name, tag=tag)
        self.verify_http_code_is(OK)
        objects0 = self.parsed.get_objects()
        assert (objects0 and len(objects0) == 1), 'failed with tag=%r' % (tag,)

        self.delete_object(bucketName=bucket_name, key=self.obj_key,
                           version=objects0[0]['versionNumber'], purge=True)
        self.verify_http_code_is(OK)
        self.get_objects(bucket_name, tag=tag)
        self.verify_http_code_is(OK)
        self.assertEquals(self.parsed.get_objects(), False,
                          'failed with tag=%r, after purging' % (tag,))
        self.get_objects(bucket_name, tag=tag, includeDeleted=True)
        self.verify_http_code_is(OK)
        self.assertEquals(self.parsed.get_objects(), False,
                          'failed with tag=%r includeDeleted=True, after purging' % (tag,))
        self.get_objects(bucket_name, tag=tag, includePurged=True)
        self.verify_http_code_is(OK)
        objects1 = self.parsed.get_objects()
        self.assertTrue(objects1 and len(objects1) >= 1,
                        'failed with tag=%r includePurged=true, after purging' % (tag,))
        self.assertEquals(objects1[-1]['status'], 'PURGED',
                          'wrong status after purged: %r' % (objects1[-1]['status'],))
        for key in objects0[0].keys():
            if key != "status" and key != "timestamp":
                if key == "reproxyURL":
                    self.assertEquals(set(objects0[0][key]), set(objects1[-1][key]),
                                      'wrong attribute %r: %r != %r' % (
                                          key, objects0[0][key], objects1[-1][key]))
                else:
                    self.assertEquals(objects0[0][key], objects1[-1][key],
                                      'wrong attribute %r: %r != %r' % (
                                          key, objects0[0][key], objects1[-1][key]))

    def test_get_objects_invalid_bucket(self):
        """
        Failed to list object if bucket not found.
        """
        logging.info('Tests GET /objects invalid bucket')
        bucket_name = 'bucket{0!s}'.format(self.create_key())
        self.get_objects(bucket_name)
        self.verify_http_code_is(NOT_FOUND)

    def test_get_objects_no_bucket(self):
        """
        Failed to list object if no bucket_name supplied.
        """
        logging.info('Tests GET /objects not bucket')
        self.get_objects()
        self.verify_http_code_is(BAD_REQUEST)

    def test_get_objects_meta(self):
        """
        Get object metadata
        """
        logging.info('Tests GET /objects metadata')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)

        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download,
                          create='new', files=(('file', StringIO('test content')),))
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.verify_get_object_meta(contentType='text/plain', downloadName=download,
                                    versionNumber=version)

    def test_get_objects_meta_version(self):
        """
        Get object metadata for specific version
        """
        logging.info('Tests GET /objects metadata version')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download,
                          create='new', files=[('file', StringIO('test content'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)

        time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName="updated" + download,
                          create='version', files=[('file', StringIO('test content updated'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.verify_get_object_meta(contentType='text/plain', downloadName="updated" + download,
                                    versionNumber=version)
        self.get_object_meta(bucket_name, key=self.obj_key, version=version - 1)
        self.verify_http_code_is(OK)
        self.verify_get_object_meta(contentType='text/plain', downloadName=download,
                                    versionNumber=version - 1)

    def test_get_objects_meta_not_found(self):
        """
        Fail to get metadata if bucket or object not found.
        """
        logging.info('Tests GET /objects meta not found')
        bucket_name = BucketsJson.get_bucket_name()
        logging.info(bucket_name)
        self.obj_key = self.get_object_key()
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)
        bucket_name = 'testbucket{0!s}'.format(self.create_key())
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)

    def test_get_objects_meta_no_bucket_no_key(self):
        """
        Fail to get object metadata without bucket_name, or without key, or without both.
        """
        logging.info('Tests GET /objects meta not bucket not key')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)

        self.get_object_meta(key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)

        self.get_object_meta(bucket_name)
        self.verify_http_code_is(NOT_FOUND)

        self.get_object_meta()
        self.verify_http_code_is(NOT_FOUND)

        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.verify_get_object_meta(contentType='text/plain', downloadName=download,
                                    versionNumber=version)

    def test_delete_objects_with_version(self):
        logging.info('Tests DELETE /objects with version')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.delete_object(bucketName=bucket_name, key=self.obj_key, version=version, purge=True)
        self.verify_http_code_is(OK)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)

    # def test_delete_objects_with_tag(self):
    #   logging.info('Tests DELETE /objects with tag')
    #   bucket_name = BucketsJson.get_bucket_name()
    #   self.obj_key = self.get_object_key()
    #   tag = 'tag-' + self.obj_key
    #   download = '{0!s}.txt'.format(self.obj_key)
    #   self.post_objects(bucketName=bucket_name, key=self.obj_key,
    #                     contentType='text/plain', downloadName=download, tag=tag,
    #                     create='new', files=[('file', BytesIO(b'test content'))])
    #   self.verify_http_code_is(CREATED)
    #   self.get_object_meta(bucket_name, key=self.obj_key)
    #   self.verify_http_code_is(OK)
    #   self.delete_object(bucketName=bucket_name, key=self.obj_key, tag=tag, purge=True)
    #   self.verify_http_code_is(OK)
    #   self.get_object_meta(bucket_name, key=self.obj_key)
    #   self.verify_http_code_is(NOT_FOUND)

    def test_delete_objects_with_uuid(self):
        logging.info('Tests DELETE /objects with uuid')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        uuid = self.parsed.get_objectAttribute("uuid")
        self.delete_object(bucketName=bucket_name, key=self.obj_key, uuid=uuid, purge=True)
        self.verify_http_code_is(OK)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)

    def test_delete_objects_only_key(self):
        logging.info('Tests DELETE /objects only key')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        self.delete_object(bucketName=bucket_name, key=self.obj_key, purge=True)
        self.verify_http_code_is(BAD_REQUEST)

    def test_delete_objects_not_found(self):
        logging.info('Tests DELETE /objects invalid key')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        self.obj_key += "none"  # sometime due to failed tests, previous object remains.
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(NOT_FOUND)

    def test_delete_objects_last_version(self):
        logging.info('Tests DELETE /objects last version')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        time.sleep(1)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName="updated" + download,
                          create='version', files=[('file', BytesIO(b'test content updated'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.delete_object(bucketName=bucket_name, key=self.obj_key, version=version, purge=True)
        self.verify_http_code_is(OK)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        self.verify_get_object_meta(versionNumber=version - 1)

    def test_delete_objects_purge_or_not(self):
        logging.info('Tests DELETE /objects purge or not')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        tag = 'tag-' + self.obj_key
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName=download, tag=tag,
                          create='new', files=[('file', StringIO('test content'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.delete_object(bucketName=bucket_name, key=self.obj_key, version=version, purge=True)
        self.verify_http_code_is(OK)
        self.get_object_meta(bucket_name, key=self.obj_key, version=version)
        self.verify_http_code_is(NOT_FOUND)
        self.get_objects(bucket_name, tag=tag, includePurged=True)
        self.verify_http_code_is(OK)
        objects = self.parsed.get_objects()
        self.assertTrue(objects and len(objects) >= 1,
                        'failed to get purged objects: %r' % (objects and len(objects),))
        self.assertEquals(objects[-1]['status'], 'PURGED',
                          'status is not correct after purge: %r' % (objects[-1]['status'],))

        key = 'testobject{0!s}'.format(self.create_key())
        download = '{0!s}.txt'.format(key)
        tag = 'tag-' + key
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=key, tag=tag,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]
        self.delete_object(bucketName=bucket_name, key=key, version=version, purge=False)
        self.verify_http_code_is(OK)
        self.get_object_meta(bucket_name, key=key, version=version)
        self.verify_http_code_is(NOT_FOUND)
        self.get_objects(bucket_name, tag=tag, includeDeleted=True)
        self.verify_http_code_is(OK)
        objects = self.parsed.get_objects()
        assert objects, 'failed to get deleted objects: {0!r}'.format(objects)
        assert objects[-1]['status'] == 'DELETED', 'status is not correct after delete: {0!r}' \
            .format(objects[-1]['status'])

    def test_delete_objects_no_bucket_no_key(self):
        """
        Fail to delete object with invalid bucket_name or key, or empty, or for both.
        """
        logging.info('Tests DELETE /objects not bucket not key')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version = self.parsed.get_objectVersionNumber()[0]

        # invalid bucket, valid key
        bucket_name1 = 'testbucket{0!s}'.format(self.create_key())
        self.delete_object(bucketName=bucket_name1, key=self.obj_key, version=version, purge=True)
        self.verify_http_code_is(NOT_FOUND)

        # valid bucket, invalid key
        key1 = 'testobject{0!s}'.format(self.create_key())
        self.delete_object(bucketName=bucket_name, key=key1, version=version, purge=True)
        self.verify_http_code_is(NOT_FOUND)

        # invalid bucket, invalid key
        self.delete_object(bucketName=bucket_name1, key=key1, version=version, purge=True)
        self.verify_http_code_is(NOT_FOUND)

        # valid bucket, no key
        self.delete_object(bucketName=bucket_name, version=version, purge=True)
        self.verify_http_code_is(BAD_REQUEST)

        # no bucket, valid key
        self.delete_object(key=self.obj_key, version=version, purge=True)
        self.verify_http_code_is(NOT_ALLOWED)

        # no bucket, no key
        self.delete_object(version=version, purge=True)
        self.verify_http_code_is(NOT_ALLOWED)

    def test_get_objects_versions(self):
        logging.info('Tests GET /objects versions')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version0 = self.parsed.get_objectVersionNumber()[0]
        time.sleep(1)
        self.post_objects(bucketName=bucket_name, key=self.obj_key,
                          contentType='text/plain', downloadName="updated" + download,
                          create='version', files=[('file', BytesIO(b'updated test content'))])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        version1 = self.parsed.get_objectVersionNumber()[0]

        self.get_object_versions(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        versions = self.parsed.get_objectVersionNumber()
        self.assertEquals(versions and len(versions), 2,
                          "incorrect number of versions: %r != 2" % (versions and len(versions),))
        self.assertEquals(versions[0], version0,
                          "incorrect first version: %r != %r" % (versions[0], version0))
        self.assertEquals(versions[1], version1,
                          "incorrect first version: %r != %r" % (versions[1], version1))
        self.delete_object(bucket_name, key=self.obj_key, version=version0, purge=True)
        self.verify_http_code_is(OK)

        self.get_object_versions(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        versions = self.parsed.get_objectVersionNumber()
        self.assertEquals(versions and len(versions), 1,
                          "incorrect number of versions after one delete: %r != 1" % (
                              versions and len(versions),))
        self.assertEquals(versions[0], version1,
                          "incorrect first version after one delete: %r != %r" % (
                              versions[0], version1))

        self.delete_object(bucket_name, key=self.obj_key, version=version1, purge=True)
        self.verify_http_code_is(OK)

        self.get_object_versions(bucketName=bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)
        versions = self.parsed.get_objectVersionNumber()
        self.assertEquals(versions and len(versions), False,
                          "incorrect number of versions after both delete: %r != False" % (
                              versions and len(versions),))

    def test_get_objects_versions_not_found(self):
        """
        Fail to get objects versions if bucket_name or key are invalid or empty, or for both.
        """
        logging.info('Tests GET /objects versions invalid bucket')
        bucket_name = BucketsJson.get_bucket_name()
        self.obj_key = self.get_object_key()
        download = '{0!s}.txt'.format(self.obj_key)
        with StringIO('test content') as f:
            self.post_objects(bucketName=bucket_name, key=self.obj_key,
                              contentType='text/plain', downloadName=download,
                              create='new', files=[('file', f), ])
        self.verify_http_code_is(CREATED)
        self.get_object_meta(bucket_name, key=self.obj_key)
        self.verify_http_code_is(OK)

        # valid bucket, invalid key
        key1 = 'testobject{0!s}'.format(self.create_key())
        self.get_object_versions(bucketName=bucket_name, key=key1)
        self.verify_http_code_is(OK)
        versions = self.parsed.get_objectVersionNumber()
        assert not versions, "valid bucket, invalid key - incorrect number of versions: " \
                             "{0!r} != False".format(versions)

        # invalid bucket, valid key
        bucket_name1 = 'testbucket{0!s}'.format(self.create_key())
        self.get_object_versions(bucketName=bucket_name1, key=self.obj_key)
        self.verify_http_code_is(OK)
        versions = self.parsed.get_objectVersionNumber()
        assert not versions, "invalid bucket, valid key - incorrect number of versions: " \
                             "{0!r} != False".format(versions)

        # invalid bucket, invalid key
        self.get_object_versions(bucketName=bucket_name1, key=key1)
        self.verify_http_code_is(OK)
        versions = self.parsed.get_objectVersionNumber()
        assert not versions, "invalid bucket, invalid key - incorrect number of versions: " \
                             "{0!r} != False".format(versions)

        # no bucket, valid key
        self.get_object_versions(key=self.obj_key)
        self.verify_http_code_is(BAD_REQUEST)
        versions = self.parsed.get_objectVersionNumber()
        assert not versions, "no bucket, valid key - incorrect number of versions: " \
                             "{0!r} != False".format(versions)

        # valid bucket, no key
        self.get_object_versions(bucketName=bucket_name, )
        self.verify_http_code_is(BAD_REQUEST)
        versions = self.parsed.get_objectVersionNumber()
        assert not versions, "valid bucket, no key - incorrect number of versions: " \
                             "{0!r} != False".format(versions)

        # no bucket, no key
        self.get_object_versions()
        self.verify_http_code_is(BAD_REQUEST)
        versions = self.parsed.get_objectVersionNumber()
        assert not versions, "no bucket, no key - incorrect number of versions: " \
                             "{0!r} != False".format(versions)

    def delete_test_objects(self):
        objects = self.get_test_objects_sql(BucketsJson.get_bucket_name())
        if objects:
            for obj in objects:
                self.delete_object(bucketName=BucketsJson.get_bucket_name(),
                                   key=str(obj[0]), uuid=str(obj[1]), purge=True)
                self.verify_http_code_is(OK)

    def get_objects_json(self):
        # Get JSON objects
        objects_records = []
        if self.obj_key:
            self.get_object_versions(bucketName=BucketsJson.get_bucket_name(), key=self.obj_key)
            objects = self.parsed.get_objects()
            if objects:
                for obj in objects:
                    objects_records.append({'key': obj['key'], 'uuid': obj['uuid']})
        return objects_records

    def get_object_key(self):
        return 'testobject{0!s}'.format(self.create_key())
        # return 'testobject{0!s}'.format(datetime.now().strftime('%Y%m%d_%H%M%S_%f'))

        # @staticmethod
        # def create_key():
        #     return datetime.now().strftime('%Y%m%d_%H%M%S_%f')


if __name__ == '__main__':
    ObjectsJson._run_tests_randomly()
