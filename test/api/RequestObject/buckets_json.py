#!/usr/bin/env python2

"""
Base class for CREPO Bucket JSON related services
"""

__author__ = 'jgray@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs

BUCKETS_API = API_BASE_URL + '/buckets'
DEFAULT_HEADERS = {'Accept': 'application/json'}
HEADER = '-H'

# Http Codes
OK = 200
CREATED = 201
BAD_REQUEST = 400

class BucketsJson(BaseServiceTest):

  def post_bucket(self):
    """
    Calls CREPO API to post bucket
    :param
    :return:JSON response
    """
    self.get_bucket()
    if self.parsed.get_bucketName() == False:
      data = {'name': self.get_bucket_name()}
      self.doPost('%s' % BUCKETS_API, data, None, DEFAULT_HEADERS)
      self.parse_response_as_json()
      self.verify_bucket_post()
    else:
      print 'The bucket already exists'

  def post_bucket_exist(self):
    """
    Calls CREPO API to post bucket that already exist
    :param
    :return:JSON response
    """
    data = {'name': self.get_bucket_name()}
    self.doPost('%s' % BUCKETS_API, data, None, DEFAULT_HEADERS)
    self.parse_response_as_json()
    self.verify_http_status(BAD_REQUEST)


  def post_bucket_without_bucketName(self):
    """
    Calls CREPO API to post bucket without bucketName specified
    :param
    :return:JSON response
    """
    self.doPost('%s' % BUCKETS_API, None, None, DEFAULT_HEADERS)
    self.parse_response_as_json()
    self.verify_http_status(BAD_REQUEST)


  def get_buckets(self):
    """
    Calls CREPO API to get bucket list
    :param
    :return:JSON response
    """
    header = {'header': HEADER}
    self.doGet('%s' % BUCKETS_API, header, DEFAULT_HEADERS)
    self.parse_response_as_json()

  def get_bucket(self):
    """
    Calls CREPO API to get a bucket by name
    :param
    :return:JSON response
    """
    header = {'header': HEADER}
    self.doGet('%s' % BUCKETS_API +'/' + self.get_bucket_name()  , header, DEFAULT_HEADERS)
    self.parse_response_as_json()
    self.verify_http_status(OK)


  @needs('parsed', 'parse_response_as_json()')
  def verify_bucket_post(self):
    """
    Verifies a valid response to api request POST /bucket
    by validating the corpus bucket specific to either our
    development or performance stack environments.
    :param API_BASE_URL from Base.Config or environment variable
    :return: Success or Error msg on Failure
    """
    print ('Validating bucket post...')
    self.verify_http_status(CREATED)
    expected_bucket = self.get_bucket_name()
    actual_buckets = self.parsed.get_bucketName()
    self.assertTrue(expected_bucket in actual_buckets, expected_bucket + ' not found in ' + unicode(actual_buckets))

  @needs('parsed', 'parse_response_as_json()')
  def verify_buckets_list(self):
    """
    Verifies a valid response to api request GET /buckets
    by validating the collection list size.
    :param API_BASE_URL from Base.Config or environment variable
    :return: Success or Error msg on Failure
    """
    print ('Validating buckets get list...')
    self.verify_http_status(OK)
    self.assertIsNotNone(self.parsed.get_buckets())
    buckets = self.parsed.get_buckets()
    if(buckets and len(buckets) > 0):
      print 'The bucket has %s element(s)' % len(buckets)
    else:
      print 'The bucket list is empty'


  @needs('parsed', 'parse_response_as_json()')
  def verify_http_status(self, httpCode):
    """
    Verifies API response according to http reponse code
    :param
    :return: Error msg on Failure
    """
    print ('Validating HTTP Status...')
    print (self.parsed.get_json())

    if(httpCode == OK or httpCode == CREATED):
      self.assertIsNotNone(self.parsed.get_bucketName())
      self.assertIsNotNone(self.parsed.get_bucketCreationDate())
      self.assertIsNotNone(self.parsed.get_bucketTimestamp())
    else:
      self.assertIsNotNone(self.parsed.get_repoErroCode())
      self.assertIsNotNone(self.parsed.get_message())
      print ('ErrorCode: ' +  str(self.parsed.get_repoErroCode()[0]) + ' - Message: ' + self.parsed.get_message()[0])

  @staticmethod
  def get_bucket_name():
    """
    Get the bucketName according our development or performance stack environments
    :return: bucketName string
    """
    bucket_name = u'corpus'
    if(API_BASE_URL == 'http://sfo-perf-plosrepo01.int.plos.org:8002'):
      bucket_name = u'mogilefs-prod-repo'
    elif(API_BASE_URL == 'http://rwc-prod-plosrepo.int.plos.org:8002'):
      bucket_name = u'mogilefs-prod-repo'
    return bucket_name