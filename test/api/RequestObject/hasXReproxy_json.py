#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2014-2019 Public Library of Science
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
Base class for CREPO hasXReproxy JSON related services
"""

import logging

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from .buckets_json import DEFAULT_HEADERS
from .config_json import CONFIG_API

__author__ = 'gfilomeno@plos.org'

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
            logging.exception(e)

    def get_config_reproxy(self):
        self.doGet(CONFIG_API, DEFAULT_HEADERS)
        self.parse_response_as_json()
        if self.parsed.get_configHasXReproxy():
            return self.parsed.get_configHasXReproxy()[0]
        else:
            raise ValueError('\nConfiguration does not have hasXReproxy value\n')
