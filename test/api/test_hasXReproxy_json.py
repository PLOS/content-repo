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
Test cases for Content Repo hasXReproxy.

GET /hasXReproxy
Get TRUE if the crepo is using reproxy and FALSE in otherwise.

"""

import logging

from ..api.RequestObject.hasXReproxy_json import ReproxyJson

__author__ = 'gfilomeno@plos.org'


class TestHasXReproxy(ReproxyJson):
    def test_get_hasXReproxy(self):
        """
        Get reproxy information.
        """
        logging.info('\nTesting GET /hasXReproxy\n')
        self.get_hasXReproxy()
        self.verify_get_hasXReproxy()


if __name__ == '__main__':
    ReproxyJson._run_tests_randomly()
