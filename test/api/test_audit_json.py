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

__author__ = 'fcabrales'
"""
Test cases for Content Repo audit requests.
"""

from ..api.RequestObject.audit_json import AuditJson

class GetAudit(AuditJson):

  def test_audit_offset_limit(self):
    """
    Get audit API call with %offset and %limit
    This test will verify if audit api call works as expected given offset and limit
    This test will verify audit bucket,keyValue,operation and uuid in a mutually exclusive matter comparing JSON
    response with values in audit table
    """
    self.get_audit_offset_limit(0,200)
    expected_buckets = self.get_audit_offset_limit_sql_bucket(0,200)
    self.verify_audit_text_bucket(expected_buckets)

    self.get_audit_offset_limit(0,200)
    expected_key_value = self.get_audit_offset_limit_sql_key_value(0,200)
    self.verify_audit_text_key(expected_key_value)

    self.get_audit_offset_limit(0,200)
    expected_operation = self.get_audit_offset_limit_sql_operation(0,200)
    self.verify_audit_text_operation(expected_operation)

    self.get_audit_offset_limit(0,200)
    expected_uuid = self.get_audit_offset_limit_sql_uuid(0,200)
    self.verify_audit_text_uuid(expected_uuid)

  def test_audit_offset(self):
    """
    Get audit API call with %offset only
    This test will verify if audit api call works as expected given only offset
    This test will verify audit bucket,keyValue,operation and uuid in a mutually exclusive matter comparing JSON
    response with values in audit table
    """
    self.get_audit_offset_only(0)
    expected_buckets = self.get_audit_offset_limit_sql_bucket(0,1000)
    self.verify_audit_text_bucket(expected_buckets)

    """
    This section of test case is comment out as per https://developer.plos.org/jira/browse/DPRO-1084
    self.get_audit_offset_only(0)
    expected_key_value = self.get_audit_offset_limit_sql_key_value(0,1000)
    self.verify_audit_text_key(expected_key_value)
    """

    self.get_audit_offset_only(0)
    expected_operation = self.get_audit_offset_limit_sql_operation(0,1000)
    self.verify_audit_text_operation(expected_operation)

    """
    This section of test case is comment out as per https://developer.plos.org/jira/browse/DPRO-1084
    self.get_audit_offset_only(0)
    expected_uuid = self.get_audit_offset_limit_sql_uuid(0,1000)
    self.verify_audit_text_uuid(expected_uuid)
    """

  def test_audit_limit(self):
    """
    Get audit API call with %limit only
    This test will verify if audit api call works as expected given only limit
    This test will verify audit bucket,keyValue,operation and uuid in a mutually exclusive matter comparing JSON
    response with values in audit table
    """
    self.get_audit_limit_only(200)
    expected_buckets = self.get_audit_offset_limit_sql_bucket(0,200)
    self.verify_audit_text_bucket(expected_buckets)

    self.get_audit_limit_only(200)
    expected_key_value = self.get_audit_offset_limit_sql_key_value(0,200)
    self.verify_audit_text_key(expected_key_value)

    self.get_audit_limit_only(200)
    expected_operation = self.get_audit_offset_limit_sql_operation(0,200)
    self.verify_audit_text_operation(expected_operation)

    self.get_audit_limit_only(200)
    expected_uuid = self.get_audit_offset_limit_sql_uuid(0,200)
    self.verify_audit_text_uuid(expected_uuid)

  def test_audit_no_parameters(self):
    """
    Get audit API call with no parameters
    This test will verify if audit api call works as expected if no parameters are given
    This test will verify audit bucket,keyValue,operation and uuid in a mutually exclusive matter comparing JSON
    response with values in audit table
    """
    self.get_audit_no_parameters()
    expected_buckets = self.get_audit_offset_limit_sql_bucket(0,1000)
    self.verify_audit_text_bucket(expected_buckets)

    """
    This section of test case is comment out as per https://developer.plos.org/jira/browse/DPRO-1084
    self.get_audit_no_parameters()
    expected_key_value = self.get_audit_offset_limit_sql_key_value(0,1000)
    self.verify_audit_text_key(expected_key_value)
    """
    self.get_audit_no_parameters()
    expected_operation = self.get_audit_offset_limit_sql_operation(0,1000)
    self.verify_audit_text_operation(expected_operation)

    """
    This section of test case is comment out as per https://developer.plos.org/jira/browse/DPRO-1084
    self.get_audit_no_parameters()
    expected_uuid = self.get_audit_offset_limit_sql_uuid(0,1000)
    self.verify_audit_text_uuid(expected_uuid)
    """
if __name__ == '__main__':
    AuditJson._run_tests_randomly()
