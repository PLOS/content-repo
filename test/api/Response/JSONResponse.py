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
Base class for Rhino's JSON based service tests.
Python's JSONPath can be installed via the following command:
  sudo pip install --allow-external jsonpath --allow-unverified jsonpath jsonpath
"""

__author__ = 'jgray@plos.org'

import logging
import json
from jsonpath import jsonpath

from .AbstractResponse import AbstractResponse


class JSONResponse(AbstractResponse):
    _json = None

    def __init__(self, response):
        try:
            self._json = json.loads(response)
        except Exception as e:
            logging.info('Error while trying to parse response as JSON!')
            logging.info('Actual response was: {0!r}'.format(response))
            raise e

    def get_json(self):
        return self._json

    def jpath(self, path):
        return jsonpath(self._json, path)

    def get_buckets(self):
        return self.jpath('$[*]')

    def get_bucketTimestamp(self):
        return self.jpath('$..timestamp')

    def get_bucketCreationDate(self):
        return self.jpath('$..creationDate')

    def get_bucketID(self):
        return self.jpath('$..bucketID')

    def get_bucketName(self):
        return self.jpath('$..bucketName')

    def get_bucketActiveObjects(self):
        return self.jpath('$..activeObjects')

    def get_bucketTotalObjects(self):
        return self.jpath('$..totalObjects')

    def get_objects(self):
        return self.jpath('$[*]')

    def get_objectKey(self):
        return self.jpath('$..objectKey')

    def get_objectDownloadName(self):
        return self.jpath('$..downloadName')

    def get_objectContentType(self):
        return self.jpath('$..contentType')

    def get_objectSize(self):
        return self.jpath('$..size')

    def get_objectVersionNumber(self):
        return self.jpath('$..versionNumber')

    def get_objectStatus(self):
        return self.jpath('$..status')

    def get_objectAttribute(self, name):
        return self._json.get(name, None)

    def get_collections(self):
        return self.jpath('$[*]')

    def get_collectionKey(self):
        return self.jpath('$..key')

    def get_collectionUUID(self):
        return self.jpath('$..uuid')

    def get_collectionStatus(self):
        return self.jpath('$..status')

    def get_collectionVersionNumber(self):
        return self.jpath('$..versionNumber')

    def get_collectionAttribute(self, name):
        return self._json.get(name, None)

    def get_repoErrorCode(self):
        return self.jpath('$..repoErrorCode')

    def get_message(self):
        return self.jpath('$..message')

    def get_all_audit_records(self):
        return self.jpath('$[*]')

    def get_audit_records_bucket(self):
        return self.jpath('$.bucket')[0]

    def get_audit_records_key(self):
        return self.jpath('$.key')[0]

    def get_audit_records_operation(self):
        return self.jpath('$.operation')[0]

    def get_audit_records_timestamp(self):
        return self.jpath('$.timestamp')[0]

    def get_audit_records_uuid(self):
        return self.jpath('$.uuid')[0]

    def get_statusBucketCount(self):
        return self.jpath('$..bucketCount')

    def get_statusServiceStarted(self):
        return self.jpath('$..serviceStarted')

    def get_statusReadsSinceStart(self):
        return self.jpath('$..readsSinceStart')

    def get_statusWritesSinceStart(self):
        return self.jpath('$..writesSinceStart')

    def get_configVersion(self):
        return self.jpath('$..version')

    def get_configObjectStore(self):
        return self.jpath('$..objectStoreBackend')

    def get_configSqlService(self):
        return self.jpath('$..sqlServiceBackend')

    def get_configHasXReproxy(self):
        return self.jpath('$..hasXReproxy')
