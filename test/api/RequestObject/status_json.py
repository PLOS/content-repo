#!/usr/bin/env python2

"""
Base class for CREPO Status JSON related services
"""

__author__ = 'gfilomeno@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from buckets_json import DEFAULT_HEADERS

STATUS_API = API_BASE_URL + '/status'

# Http Codes
OK = 200

class StatusJson(BaseServiceTest):

  def get_status(self):
    """
    Calls CREPO API to get status information
    GET /status
    """
    self.doGet(STATUS_API, DEFAULT_HEADERS)
    self.parse_response_as_json()

  def verify_status(self):
    """
    Verifies a valid response for GET /status
    """
    self.verify_http_code_is(OK)
    # Validate if the response parameters are not none
    self.assertIsNotNone(self.parsed.get_statusReadsSinceStart())
    self.assertIsNotNone(self.parsed.get_statusBucketCount())
    self.assertIsNotNone(self.parsed.get_statusServiceStarted())
    self.assertIsNotNone(self.parsed.get_statusWritesSinceStart())


