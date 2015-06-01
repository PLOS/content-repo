#!/usr/bin/env python2

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

    self.get_audit_offset_only(0)
    expected_key_value = self.get_audit_offset_limit_sql_key_value(0,1000)
    self.verify_audit_text_key(expected_key_value)

    self.get_audit_offset_only(0)
    expected_operation = self.get_audit_offset_limit_sql_operation(0,1000)
    self.verify_audit_text_operation(expected_operation)

    self.get_audit_offset_only(0)
    expected_uuid = self.get_audit_offset_limit_sql_uuid(0,1000)
    self.verify_audit_text_uuid(expected_uuid)

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

    self.get_audit_no_parameters()
    expected_key_value = self.get_audit_offset_limit_sql_key_value(0,1000)
    self.verify_audit_text_key(expected_key_value)

    self.get_audit_no_parameters()
    expected_operation = self.get_audit_offset_limit_sql_operation(0,1000)
    self.verify_audit_text_operation(expected_operation)

    self.get_audit_no_parameters()
    expected_uuid = self.get_audit_offset_limit_sql_uuid(0,1000)
    self.verify_audit_text_uuid(expected_uuid)

if __name__ == '__main__':
    AuditJson._run_tests_randomly()