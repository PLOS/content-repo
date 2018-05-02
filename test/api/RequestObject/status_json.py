#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
Base class for CREPO Status JSON related services
"""

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from .buckets_json import DEFAULT_HEADERS

__author__ = 'gfilomeno@plos.org'

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
