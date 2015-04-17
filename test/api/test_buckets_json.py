#!/usr/bin/env python2

__author__ = 'jgray@plos.org'

'''
Test cases for Content Repo Bucket requests.
'''
from ..api.RequestObject.buckets_json import BucketsJson


class TestBuckets(BucketsJson):

  def test_post_bucket(self):
    """
    Post a new bucket
    :return:
    """
    print '*** Test POST bucket ***'
    self.post_bucket()

  def _test_post_bucket_without_bucketName(self):
    """
    Post a new bucket without bucketName defined
    :return:
    """
    print '*** Test POST bucket without bucket name***'
    self.post_bucket_without_bucketName()


  def test_post_bucket_exist(self):
    """
    Post a new bucket that already exists
    :return:
    """
    print '*** Test POST bucket already exists***'
    self.post_bucket_exist()

  def test_get_bucket_by_name(self):
    """
    Get Bucket by name API call
    """
    print '*** Test GET bucket by name***'
    self.get_bucket()

  def test_get_buckets_list(self):
    """
    Get Buckets List API call
    """
    print '*** Test GET bucket list***'
    self.get_buckets()
    self.verify_buckets_list()

if __name__ == '__main__':
    BucketsJson._run_tests_randomly()
