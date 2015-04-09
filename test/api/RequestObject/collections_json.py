#!/usr/bin/env python2

"""
Base class for CREPO Collection JSON related services
"""

__author__ = 'gabriela.filomeno'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs

COLLECTIONS_API = API_BASE_URL + '/v1/collections'
DEFAULT_HEADERS = {'Accept': 'application/json'}
HEADER = '-H'

class CollectionsJson(BaseServiceTest):

    def get_collections(self):
        """
        Calls CREPO API to GET collections list
        :param bucketName The Collection's bucket name
        :return:JSON response
        """
        params = {'bucketName': self.get_bucket_name()}
        self.doGet('%s' % COLLECTIONS_API, params, DEFAULT_HEADERS)
        self.parse_response_as_json()

    @needs('parsed', 'parse_response_as_json()')
    def verify_list_collections(self):
        """
        Verifies a valid response to api request GET/POST collections
        :param API_BASE_URL from Base.Config or environment variable
        :return: Success or Error msg on Failure
        """
        print ('Validating collections...'),
        collections = self.parsed.get_collections()
        self.assertIsNotNone(collections)
        for i in range(len(collections)):
            print collections[i]

    # Get the bucketName according our development or performance stack environments
    def get_bucket_name(self):
        bucket_name = u'corpus'
        if(API_BASE_URL == 'http://sfo-perf-plosrepo01.int.plos.org:8002'):
            bucket_name = u'mogilefs-prod-repo'
        elif(API_BASE_URL == 'http://rwc-prod-plosrepo.int.plos.org:8002'):
            bucket_name = u'mogilefs-prod-repo'
        return bucket_name



