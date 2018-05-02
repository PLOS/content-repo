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
Base class for CREPO audit JSON related services
"""

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from ...Base.MySQL import MySQL

__author__ = 'fcabrales'

AUDIT_API = API_BASE_URL + '/audit'
DEFAULT_HEADERS = {'Accept': 'application/json'}
HEADER = '-H'


class AuditJson(BaseServiceTest):
    def get_audit_offset_limit(self, offset, limit):
        """
        Calls CREPO API to get audit list
        GET /audit
        Calls audit API with parameters
        :param offset: offset to be used to get audit records
        :param limit: limit on the amount of records to return
        :return:JSON response
        """
        daData = {'header': HEADER, 'offset': offset, 'limit': limit}
        self.doGet('%s' % (AUDIT_API), daData, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def get_audit_offset_only(self, offset):
        """
        Calls CREPO API to get audit list
        GET /audit
        Calls audit API with parameters
        :param offset: offset to be used to get audit records
        :return:JSON response
        """
        daData = {'header': HEADER, 'offset': offset}
        self.doGet('%s' % (AUDIT_API), daData, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def get_audit_limit_only(self, limit):
        """
        Calls CREPO API to get audit list
        GET /audit
        Calls audit API with parameters
        :param limit: limit on the amount of records to return
        :return:JSON response
        """
        daData = {'header': HEADER, 'limit': limit}
        self.doGet('%s' % (AUDIT_API), daData, DEFAULT_HEADERS)
        self.parse_response_as_json()

    def get_audit_no_parameters(self):
        """
        Calls CREPO API to get audit list
        GET /audit
        Calls audit API with parameters
        :return:JSON response
        """
        daData = {'header': HEADER}
        self.doGet('%s' % (AUDIT_API), daData, DEFAULT_HEADERS)
        self.parse_response_as_json()

    @needs('parsed', 'parse_response_as_json()')
    def verify_audit_text_bucket(self, expected_results_bucket):
        self.verify_audit_text(expected_results_bucket, 'bucket')

    @needs('parsed', 'parse_response_as_json()')
    def verify_audit_text_key(self, expected_results_key):
        self.verify_audit_text(expected_results_key, 'key')

    @needs('parsed', 'parse_response_as_json()')
    def verify_audit_text_operation(self, expected_results_operation):
        self.verify_audit_text(expected_results_operation, 'operation')

    @needs('parsed', 'parse_response_as_json()')
    def verify_audit_text_timestamp(self, expected_results_timestamp):
        self.verify_audit_text(expected_results_timestamp, 'timestamp')

    @needs('parsed', 'parse_response_as_json()')
    def verify_audit_text_uuid(self, expected_results_uuid):
        self.verify_audit_text(expected_results_uuid, 'uuid')

    """
    Verify audit text will accept the expected db records and the attribute for this records
    it will then compare db records wil actual values in json response for attribute
    """

    @needs('parsed', 'parse_response_as_json()')
    def verify_audit_text(self, expected_results_full_text, attribute):
        print('Validating  audit field for ' + attribute)
        actual_source = self.parsed.get_all_audit_records()
        array_expected_result = []
        decoded = [[word.decode("utf8") for word in sets] for sets in expected_results_full_text]
        for i in decoded:
            array_expected_result.append(u" ".join(i))
        for actual_full_text_article, expected_full_text_article in zip(actual_source,
                                                                        array_expected_result):
            assert actual_full_text_article[attribute] == expected_full_text_article, (
                '%s is not correct! actual: %s expected: %s' % (
                attribute, actual_full_text_article[attribute], expected_full_text_article))

    """
    Below SQL statements will query CREPO audit table for bucketName attribute given limit and offset
    """

    def get_audit_offset_limit_sql_bucket(self, offset, limit):
        expected_results_bucket = (MySQL().query(
            "SELECT bucketName FROM audit a ORDER BY a.id ASC LIMIT " + str(
                limit) + " OFFSET " + str(offset)))
        return expected_results_bucket

    """
    Below SQL statements will query CREPO audit table for keyValue attribute given limit and offset
    """

    def get_audit_offset_limit_sql_key_value(self, offset, limit):
        expected_results_key_value = (MySQL().query(
            "SELECT keyValue FROM audit a ORDER BY a.id ASC LIMIT " + str(limit) + " OFFSET " + str(
                offset)))
        return expected_results_key_value

    """
    Below SQL statements will query CREPO audit table for operation attribute given limit and offset
    """

    def get_audit_offset_limit_sql_operation(self, offset, limit):
        expected_results_operation = (MySQL().query(
            "SELECT operation FROM audit a ORDER BY a.id ASC LIMIT " + str(
                limit) + " OFFSET " + str(offset)))
        return expected_results_operation

    """
    Below SQL statements will query CREPO audit table for uuid attribute given limit and offset
    """

    def get_audit_offset_limit_sql_uuid(self, offset, limit):
        expected_results_uuid = (MySQL().query(
            "SELECT uuid FROM audit a ORDER BY a.id ASC  LIMIT " + str(limit) + " OFFSET " + str(
                offset)))
        return expected_results_uuid

    """
    Below SQL statements will query CREPO audit table for timestamp attribute given limit and offset
    """

    def get_audit_offset_limit_sql_timestamp(self, offset, limit):
        expected_results_timestamp = (MySQL().query(
            "SELECT timestamp FROM audit a ORDER BY a.id ASC LIMIT " + str(
                limit) + " OFFSET " + str(offset)))
        return expected_results_timestamp
