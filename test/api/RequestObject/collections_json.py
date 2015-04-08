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

    # Define a default bucketName
    bucketName = u'corpus'

    # Set the bucketName according our development or performance stack environments
    def set_bucketName():
        global bucketName
        if(API_BASE_URL == 'http://sfo-perf-plosrepo01.int.plos.org:8002'):
            bucketName = u'mogilefs-prod-repo'
        elif(API_BASE_URL == 'http://rwc-prod-plosrepo.int.plos.org:8002'):
            bucketName = u'mogilefs-prod-repo'

    # Call the set bucketName method
    set_bucketName()

    # Request for GET collections endpoint
    def get_collections(self):
        """
        Calls CREPO API to get collections list
        :param
        :return:JSON response
        """
        header = {'header': HEADER}
        self.doGet('%s' % COLLECTIONS_API + '?bucketName=' + self.bucketName, header, DEFAULT_HEADERS)
        self.parse_response_as_json()

    @needs('parsed', 'parse_response_as_json()')
    def verify_collections(self):
        """
        Verifies a valid response to api request GET /collections
        :param API_BASE_URL from Base.Config or environment variable
        :return: Success or Error msg on Failure
        """
        print ('Validating collections...'),
        actual_collectionKey = self.parsed.get_collectionKey()
        print(unicode(actual_collectionKey))
