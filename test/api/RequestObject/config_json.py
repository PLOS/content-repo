#!/usr/bin/env python2

"""
Base class for CREPO config JSON service
"""

__author__ = 'gfilomenoh@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from buckets_json import DEFAULT_HEADERS

CONFIG_API = API_BASE_URL + '/config'

# Http Codes
OK = 200

class ConfigJson(BaseServiceTest):

  object_stores = ["FileSystemStoreService", "InMemoryFileStoreService", "MogileStoreService", "S3StoreService"]

  def get_config(self):
    """
    Calls CREPO API to get configuration information
    GET /config
    """
    self.doGet(CONFIG_API, DEFAULT_HEADERS)
    self.parse_response_as_json()

  @needs('parsed', 'parse_response_as_json()')
  def verify_get_config(self):
    """
    Verifies a valid response for GET /config.
    """
    self.verify_http_code_is(OK)
    # Validate only if the version is present, the version.properties is in the server side
    self.assertIsNotNone(self.parsed.get_configVersion())
    self.assertIsNotNone(self.parsed.get_configObjectStore())
    # Validate if the object store is into the available options
    self.assertIn(self.parsed.get_configObjectStore()[0].rsplit('.', 1)[-1], self.object_stores)
    self.assertIsNotNone(self.parsed.get_configSqlService())
    self.assertIsNotNone(self.parsed.get_configHasXReproxy())