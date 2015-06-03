#!/usr/bin/env python2

__author__ = 'gfilomeno@plos.org'

"""
Test cases for Content Repo Config requests.

GET /config
Information about configuration

"""
from ..api.RequestObject.config_json import  ConfigJson


class TestConfig(ConfigJson):

  def test_get_config(self):
    """
    Get configuration
    """
    self.get_config()
    self.verify_get_config()


if __name__ == '__main__':
  ConfigJson._run_tests_randomly()
