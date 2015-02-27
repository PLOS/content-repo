#!/usr/bin/env python2

"""
Base class for CREPO Bucket JSON related services
"""

__author__ = 'jgray@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs

BUCKETS_API = API_BASE_URL + '/v1/buckets'
DEFAULT_HEADERS = {'Accept': 'application/json'}
HEADER = '-H'


class BucketsJson(BaseServiceTest):

  def get_buckets(self):
    """
    Calls CREPO API to get bucket list
    :param
    :return:JSON response
    """
    header = {'header': HEADER}
    self.doGet('%s' % BUCKETS_API, header, DEFAULT_HEADERS)
    self.parse_response_as_json()

  @needs('parsed', 'parse_response_as_json()')
  def verify_buckets(self):
    """
    Verifies a valid response to api request GET /buckets
    by validating the corpus bucket specific to either our
    development or performance stack environments.

    :param API_BASE_URL from Base.Config or environment variable
    :return: Success or Error msg on Failure
    """
    if(API_BASE_URL == 'http://sfo-perf-plosrepo01.int.plos.org:8002'):
      expected_bucket = u'mogilefs-prod-repo'
    elif(API_BASE_URL == 'http://rwc-prod-plosrepo.int.plos.org:8002'):
      expected_bucket = u'mogilefs-prod-repo'
    else:
      expected_bucket = u'corpus'

    print ('Validating buckets...'),
    actual_buckets = self.parsed.get_bucketName()
    #print(unicode(actual_buckets))
    self.assertTrue(expected_bucket in actual_buckets, expected_bucket + ' not found in ' + unicode(actual_buckets))
