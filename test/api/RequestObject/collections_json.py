#!/usr/bin/env python2

"""
Base class for CREPO Collection JSON related services
"""

__author__ = 'gabriela.filomeno'

import json
from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from buckets_json import BucketsJson

COLLECTIONS_API = API_BASE_URL + '/collections'
DEFAULT_HEADERS = {'Accept': 'application/json'}
HEADER = '-H'

class CollectionsJson(BaseServiceTest):

    def get_collections(self):
        """
        Calls CREPO API to GET collections list
        :param bucketName The Collection's bucket name
        :return:JSON response
        """
        params = {'bucketName': BucketsJson.get_bucket_name()}
        self.doGet('%s' % COLLECTIONS_API, params, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def post_collection(self):
        """
        Calls CREPO API to POST collection
        :param JSON request Input Collection
        :return:JSON response
        """
        DEFAULT_HEADERS = {'Content-Type': 'application/json'}
        self.doPost('%s' % COLLECTIONS_API, json.dumps(self.get_input_collection()), None, DEFAULT_HEADERS)
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

    @needs('parsed', 'parse_response_as_json()')
    def verify_post_collections(self):
        """
        Verifies a valid response to api request POST collection
        :param API_BASE_URL from Base.Config or environment variable
        :return: Success or Error msg on Failure
        """
        print ('Validating POST collection...'),
        expected_collection_key = self.get_coll_key()
        expected_collection_status = 'USED'

        actual_collection_key = self.parsed.get_collectionKey()
        actual_collection_status = self.parsed.get_collectionStatus()

        self.assertTrue(expected_collection_key in actual_collection_key, expected_collection_key + ' not found in ' + unicode(actual_collection_key))
        self.assertTrue(expected_collection_status in actual_collection_status, expected_collection_status + ' not found in ' + unicode(actual_collection_status))
        self.assertIsNotNone(self.parsed.get_collectionUUID())

    # Get the collection key according our development or performance stack environments
    def get_coll_key(self):
        _coll_key = u'10.1371/journal.pone.0099139'
        if(API_BASE_URL == 'http://sfo-perf-plosrepo01.int.plos.org:8002'):
            _coll_key = u'10.1371/journal.pone.0099139'
        elif(API_BASE_URL == 'http://rwc-prod-plosrepo.int.plos.org:8002'):
            _coll_key = u'10.1371/journal.pone.0099139'
        return _coll_key

    # Get the collection's objects according our development or performance stack environments
    # TODO: Once the test cases for objects will be do, we could integrate a get object call
    def get_input_object(self):
        _input_objects = []
        if(API_BASE_URL == 'http://sfo-perf-plosrepo01.int.plos.org:8002'):
            _input_objects = [{'key':'PDF/10.1371/journal.pone.0099139',
                               'uuid':'f634cf68-c37e-404e-962f-a80e3856139c'}]
        elif(API_BASE_URL == 'http://rwc-prod-plosrepo.int.plos.org:8002'):
            _input_objects = [{'key':'PDF/10.1371/journal.pone.0099139',
                               'uuid':'f634cf68-c37e-404e-962f-a80e3856139c'}]
        else:
            _input_objects = [{'key':'PDF/10.1371/journal.pone.0099139',
                               'uuid':'747dbba5-0e5d-46d2-a8ff-fb27802b8ac1'}]
        return _input_objects

    # Get the collection data
    def get_input_collection(self):
        _input_collection = {'key':self.get_coll_key(),
                             'bucketName':BucketsJson.get_bucket_name(),
                             'create':'auto',
                             'objects': self.get_input_object()}
        return _input_collection
