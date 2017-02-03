#!/usr/bin/env python2

# Copyright (c) 2017 Public Library of Science
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

"""
Base class for CREPO config JSON service
"""

__author__ = 'gfilomeno@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from buckets_json import DEFAULT_HEADERS

CONFIG_API = API_BASE_URL + '/config'

# Http Codes
OK = 200

class ConfigJson(BaseServiceTest):

  object_stores = ['FileSystemStoreService', 'InMemoryFileStoreService', 'MogileStoreService', 'S3StoreService']
  sql_service = 'MysqlService'

  def get_config(self):
    """
    Calls CREPO API to get configuration information
    GET /config
    :return JSON response
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
    object_store = ConfigJson.split_string(self.parsed.get_configObjectStore()[0])
    self.assertIn(object_store, ConfigJson.object_stores)
    self.assertIsNotNone(self.parsed.get_configSqlService())
    # Validate if the sql service is Mysql
    self.assertEquals(ConfigJson.split_string(self.parsed.get_configSqlService()[0]), ConfigJson.sql_service)
    # Validate hasXReproxy value
    self.verify_store_reproxy(object_store)

  def verify_store_reproxy(self, object_store):
    # Verify hasXReproxy value according to object store configuration
    self.assertIsNotNone(self.parsed.get_configHasXReproxy())
    if object_store in [ConfigJson.object_stores[2], ConfigJson.object_stores[3]]:
      self.assertTrue(self.parsed.get_configHasXReproxy()[0])
    elif object_store == ConfigJson.object_stores[1]:
      self.assertFalse(self.parsed.get_configHasXReproxy()[0])

  @staticmethod
  def split_string(value):
    return value.rsplit('.', 1)[-1]
