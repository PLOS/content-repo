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

__author__ = 'jkrzemien@plos.org'

from types import FunctionType
from unittest import TestCase
import Config


def __replicate_test_method(cls, original_test, new_test_name):
    """
    *Private* method that **clones** a test method, names it and inserts it in the test class
    """
    # Create a new test from the old test
    new_test = FunctionType(original_test.func_code, original_test.func_globals, name=new_test_name)

    # Copy over any other attributes the original test had
    for attr in list(set(dir(original_test)) - set(dir(new_test))):
        setattr(new_test, attr, getattr(original_test, attr))

    # Add the new test to the decorated TestCase
    setattr(cls, new_test_name, new_test)


def MultiBrowserFixture(cls):
    """
    Generates new test methods from methods defined in the decorated class
    """

    def __getBrowserName(browser):
        try:
            return browser['browserName']
        except KeyError:
            return browser['platformName']

    # Only accept classes that inherit from `TestCase`
    if not issubclass(cls, TestCase):
        raise Exception(
                'Decorated class is not a subclass of FrontEndTest! '
                'Cannot consider it a Web Driver test!')

    browsers = []
    test_case_attrs = dir(cls)
    tests = 0

    for attr_name in test_case_attrs:

        # Per method that starts with `test_`...
        if attr_name.startswith("test_"):
            tests += 1
            original_test = getattr(cls, attr_name, None)

            # Compile all enabled/available browsers
            if Config.run_against_grid == True: browsers += Config.grid_enabled_browsers
            if Config.run_against_appium == True: browsers += Config.appium_enabled_browsers

            for browser in browsers:
                # Name the new test based on original and the browser that we will run it against
                base_test_name = str(original_test.__name__)
                new_test_name = "{0}_{1}".format(base_test_name, __getBrowserName(browser))

                __replicate_test_method(cls, original_test, new_test_name)

                # Remove the original test method
                setattr(cls, base_test_name, None)

    # Inject browsers combination into `_injected_drivers` attribute
    # (in [[FrontEndTest.py#_injected_drivers]])
    setattr(cls, "_injected_drivers", browsers * tests)

    return cls
