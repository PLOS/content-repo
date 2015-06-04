#!/usr/bin/env python2

__author__ = 'gfilomeno@plos.org'

"""
Test cases for Content Repo status request.

GET /status
Get info about system status.
"""
from ..api.RequestObject.status_json import StatusJson


class TestStatus(StatusJson):

  def test_get_status(self):
    """
    Get Status information
    """
    self.get_status()
    self.verify_status()

if __name__ == '__main__':
  StatusJson._run_tests_randomly()
