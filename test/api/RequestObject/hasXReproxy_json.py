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
    self.assertIsNotNone(self.parsed.get_json())
    try:
      # Validate if /hasXReproxy value is the same at /config hasXReproxy
      self.assertEquals(self.parsed.get_json(), self.get_config_reproxy())
    except ValueError as e:
      print e


  def get_config_reproxy(self):
    self.doGet(CONFIG_API, DEFAULT_HEADERS)
    self.parse_response_as_json()
    if self.parsed.get_configHasXReproxy():
      return self.parsed.get_configHasXReproxy()[0]
    else:
      raise ValueError('\nConfiguration does not have hasXReproxy value\n')
