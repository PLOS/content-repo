#!/usr/bin/env python2

"""
Base class for CREPO hasXReproxy JSON related services
"""

__author__ = 'gfilomeno@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from buckets_json import DEFAULT_HEADERS
from config_json import CONFIG_API


REPROXY_API = API_BASE_URL + '/hasXReproxy'

# Http Codes
OK = 200


class ReproxyJson(BaseServiceTest):

  def get_hasXReproxy(self):
    """
    Calls CREPO API to ask if the crepo has reproxy
    GET /hasXReproxy
    """
    self.doGet(REPROXY_API, DEFAULT_HEADERS)
    self.parse_response_as_json()

  def verify_get_hasXReproxy(self):
    """
    Verifies a valid response for GET /hasXReproxy
    """
    self.verify_http_code_is(OK)
    # Verify hasXReproxy is not none
    self.assertIsNotNone(self.parsed.get_hasXReproxy())
    print "hasXReproxy: " + str(self.parsed.get_hasXReproxy())

  def verify_store_reproxy(self, has_reproxy):
    # Verify hasXReproxy value according to object store configuration
    self.verify_http_code_is(OK)
    self.assertIsNotNone(self.parsed.get_configObjectStore())
    object_store = self.parsed.get_configObjectStore()[0].rsplit('.', 1)[-1]
    if object_store in ['MogileStoreService', 'S3StoreService']:
      self.assertTrue(has_reproxy)
    elif object_store == 'InMemoryFileStoreService':
      self.assertFalse(has_reproxy)

  def get_config(self):
    """
    Calls CREPO API to get configuration information
    """
    self.doGet(CONFIG_API, DEFAULT_HEADERS)
    self.parse_response_as_json()
