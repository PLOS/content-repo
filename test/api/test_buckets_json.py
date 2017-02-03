#!/usr/bin/env python2

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

__author__ = 'jgray@plos.org'

"""
Test cases for Content Repo Bucket requests.

POST /buckets
Create a bucket. Error if bucket name exist.

GET /buckets
List buckets.

DELETE /buckets/{name}
Delete a bucket. Error if not empty.

GET /buckets/{bucketName}
Info about the bucket. Error if bucket name not exist.
"""
from ..api.RequestObject.buckets_json import BucketsJson, OK, CREATED, BAD_REQUEST, NOT_FOUND
import random


class TestBuckets(BucketsJson):

  def test_cleanup(self):
    """
    Cleanup any empty bucket with name starting with "testbucket"
    """
    self.get_buckets()
    for name in self.buckets:
      if name.startswith('testbucket'):
        self.delete_bucket(name)

  def test_post_bucket(self):
    """
    Create a bucket by name.

    Post a new bucket with a random name, and verify that it exists
    in subsequent get bucket request, but not before.
    """
    # self.test_cleanup()
    name = 'testbucket%d' % (random.randint(1000, 1999))
    self.get_buckets()
    self.verify_no_bucket(name)
    self.post_bucket(name)
    self.verify_http_status(CREATED)
    self.verify_post_bucket(name)
    self.get_buckets()
    self.verify_has_bucket(name)
    self.delete_bucket(name)

  def test_post_bucket_exist(self):
    """
    Fail to create a bucket with existing name.

    Post a new bucket two times, the second time fails.
    """
    # self.test_cleanup()
    name = 'testbucket%d' % (random.randint(1000, 1999))
    self.post_bucket(name)
    self.post_bucket(name)
    self.verify_http_status(BAD_REQUEST)
    self.delete_bucket(name)

  def _test_post_bucket_without_name(self):
    """
    Fail to create a bucket without a name.

    Post a new bucket without a name.
    :return:
    """
    # self.test_cleanup()
    self.post_bucket()
    self.verify_http_status(BAD_REQUEST)

  def test_get_bucket_by_name(self):
    """
    Get Bucket by name.

    Post a new bucket, and then do a get to verify it exists.
    """
    name = 'testbucket%d' % (random.randint(1000, 1999))
    self.post_bucket(name)
    self.get_bucket(name)
    self.verify_get_bucket(name)
    self.delete_bucket(name)

  def test_get_bucket_not_exist(self):
    """
    Fail to get bucket that does not exist.

    Verify that a name does not exist, and then do a get to verify it fails.
    """
    name = 'testbucket%d' % (random.randint(1000, 1999))
    self.get_buckets()
    self.verify_no_bucket(name)
    self.get_bucket(name)
    self.verify_http_status(NOT_FOUND)

  def test_get_buckets(self):
    """
    Get Buckets List API call.

    Post two buckets, and verify that get buckets has those.
    """
    name1 = 'testbucket%d' % (random.randint(1000, 1999))
    name2 = 'testbucket%d' % (random.randint(2000, 2999))
    self.get_buckets()
    self.verify_no_bucket(name1)
    self.verify_no_bucket(name2)
    self.post_bucket(name1)
    self.post_bucket(name2)
    self.get_buckets()
    self.verify_http_status(OK)
    self.verify_has_bucket(name1)
    self.verify_has_bucket(name2)
    self.delete_bucket(name1)
    self.delete_bucket(name2)

  def test_get_buckets_has_default(self):
    """
    Get Buckets List and make sure it has default bucket
    """
    name = self.get_bucket_name()
    self.get_buckets()
    self.verify_http_status(OK)
    self.verify_has_bucket(name)

  def test_delete_bucket(self):
    """
    Delete a bucket by name.

    Post a new bucket, and then delete it, and verify that it does not exist anymore.
    """
    # self.test_cleanup()
    name = 'testbucket%d' % (random.randint(1000, 1999))
    self.post_bucket(name)
    self.get_buckets()
    self.verify_has_bucket(name)
    self.delete_bucket(name)
    self.verify_http_status(OK)
    self.get_buckets()
    self.verify_no_bucket(name)

  def test_delete_bucket_not_exist(self):
    """
    Fail to delete a bucket that does not exist.

    Try to delete a bucket that does not exist.
    """
    # self.test_cleanup()
    name = 'testbucket%d' % (random.randint(1000, 1999))
    self.get_buckets()
    self.verify_no_bucket(name)
    self.delete_bucket(name)
    self.verify_http_status(NOT_FOUND)

  def test_delete_bucket_not_empty(self):
    """
    Fail to delete a bucket that is not empty.

    Since this test case will create unnecessary buckets that cannot be deleted,
    I use the default bucket to test deletion.
    """
    name = self.get_bucket_name()
    self.get_bucket(name)
    self.verify_http_status(OK)
    self.verify_get_bucket(name)
    self.verify_default_bucket()
    self.delete_bucket(name)
    self.verify_http_status(BAD_REQUEST)


if __name__ == '__main__':
    BucketsJson._run_tests_randomly()
