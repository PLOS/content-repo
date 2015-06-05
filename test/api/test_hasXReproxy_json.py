#!/usr/bin/env python2

__author__ = 'gfilomeno@plos.org'

"""
Test cases for Content Repo hasXReproxy.

GET /hasXReproxy
Get TRUE if the crepo is using reproxy and FALSE in otherwise.

"""
from ..api.RequestObject.hasXReproxy_json import ReproxyJson


class TestHasXReproxy(ReproxyJson):


  def test_get_hasXReproxy(self):
    """
    Get reproxy information.
    """
    self.get_hasXReproxy()
    self.verify_get_hasXReproxy()
    has_reproxy = self.parsed.get_hasXReproxy()
    self.get_config()
    self.verify_store_reproxy(has_reproxy)

if __name__ == '__main__':
  ReproxyJson._run_tests_randomly()
