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
Base class for CREPO Bucket JSON related services
"""

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs

__author__ = 'jgray@plos.org'

BUCKETS_API = API_BASE_URL + '/buckets'
DEFAULT_HEADERS = {'Accept': 'application/json'}
HEADER = '-H'

# get BUCKET_NAME based on whether API_BASE_URL is on prod or not.
BUCKET_NAME = 'testbucketplos20191008-00'  # 'corpus'
# if API_BASE_URL.split('/')[2] in ('contentrepo-201.sfo.plos.org:8002'):
#     BUCKET_NAME = 'mogilefs-prod-repo'

# Http Codes
OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404


class BucketsJson(BaseServiceTest):
    def get_buckets(self):
        """
        Calls CREPO API to get buckets list
        GET /buckets
        """
        header = {'header': HEADER}
        self.doGet('{0!s}'.format(BUCKETS_API), header, DEFAULT_HEADERS)
        self.parse_response_as_json()
        self.buckets = [x['bucketName'] for x in self.parsed.get_buckets()]

    def post_bucket(self, name=None):
        """
        Calls CREPO API to create bucket by name.
        POST /buckets form: name={name}

        :param name: optional bucket name.
        """
        data = None
        if name:
            data = {'name': name}
        self.doPost('{0!s}'.format(BUCKETS_API), data, None, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def get_bucket(self, name):
        """
        Calls CREPO API to get a bucket by name
        GET /buckets/{name}

        :param name: bucket name.
        """
        self.doGet('{0!s}/{1!s}'.format(BUCKETS_API, name), None, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def delete_bucket(self, name):
        """
        Calls CREPO API to get a bucket by name
        DELETE /buckets/{name}

        :param name: bucket name.
        """
        self.doDelete('{0!s}/{1!s}'.format(BUCKETS_API, name), None, DEFAULT_HEADERS)

    @needs('parsed', 'parse_response_as_json()')
    def verify_get_buckets(self):
        """
        Verifies a valid response for GET /buckets and fills self.buckets.
        """
        self.verify_http_status(OK)

    def verify_has_bucket(self, name):
        """
        Verifies that the name exists in buckets.
        """
        assert name in self.buckets, '{0!s} not found in {1!s}'.format(name, self.buckets)

    def verify_no_bucket(self, name):
        """
        Verifies that the name does not exists in buckets.
        """
        assert name not in self.buckets, '{0!s} found in {1!s}'.format(name, self.buckets)

    @needs('parsed', 'parse_response_as_json()')
    def verify_get_bucket(self, name):
        """
        Verifies a valid response to api request GET /buckets/{name}
        """
        actual_bucket = self.parsed.get_bucketName()
        assert name in actual_bucket, \
            '{0!s} is not found in {1!s}'.format(name, actual_bucket)

    @needs('parsed', 'parse_response_as_json()')
    def verify_post_bucket(self, name):
        """
        Verifies a valid response to api request POST /buckets
        """
        self.verify_http_status(CREATED)
        actual_bucket = self.parsed.get_bucketName()
        assert name in actual_bucket, '{0!s} is not in {1!s}'.format(name, actual_bucket)

    @needs('parsed', 'parse_response_as_json()')
    def verify_http_status(self, httpCode):
        """
        Verifies API response according to http response code
        :param
        :return: Error msg on Failure
        """
        self.verify_http_code_is(httpCode)
        if httpCode == OK or httpCode == CREATED:
            self.assertIsNotNone(self.parsed.get_bucketName())
            self.assertIsNotNone(self.parsed.get_bucketCreationDate())
            self.assertIsNotNone(self.parsed.get_bucketTimestamp())
        else:
            self.assertIsNotNone(self.parsed.get_repoErrorCode())
            self.assertIsNotNone(self.parsed.get_message())

    def verify_default_bucket(self):
        """
        Verify that the default bucket has active objects
        """
        assert self.parsed.get_bucketActiveObjects(), '{0!r} is not valid' \
            .format(self.parsed.get_bucketActiveObjects())

    @staticmethod
    def get_bucket_name():
        """
        Get the bucketName according our development or performance stack environments
        :return: bucket name string
        """
        return BUCKET_NAME
