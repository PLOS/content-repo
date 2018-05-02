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

import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from .CustomException import ElementDoesNotExistAssertionError
from bs4 import BeautifulSoup, NavigableString
from .LinkVerifier import LinkVerifier
from .CustomExpectedConditions import elementToBeClickable
from .Config import wait_timeout, base_url, environment


class PlosPage(object):
    """
    Model an abstract base Journal page.
    """
    PROD_URL = ''

    def __init__(self, driver, urlSuffix=''):

        # Internal WebDriver-related protected members
        self._driver = driver
        self._wait = WebDriverWait(self._driver, wait_timeout)
        self._actions = ActionChains(self._driver)

        base_url = self.__buildEnvironmentURL(urlSuffix)

        # Prevents WebDriver from navigating to a page more than once (there should be only one
        # starting point for a test)
        if not hasattr(self._driver, 'navigated'):
            try:
                self._driver.get(base_url)
                self._driver.navigated = True
            except TimeoutException as toe:
                logging.info(
                        '\t[WebDriver Error] WebDriver timed out while trying to load the '
                        'requested web page {0!r}.'.format(base_url))
                raise toe

        # Internal private member
        self.__linkVerifier = LinkVerifier()

        # Locators - Instance variables unique to each instance
        self._article_type_menu = (By.ID, 'article-type-menu')

    # POM Actions

    def __buildEnvironmentURL(self, urlSuffix):
        """
        *Private* method to detect on which environment we are running the test.
        Then builds up a URL accordingly
    
        1. urlSuffix: String representing the suffix to append to the URL. It is generally provided
    
        **Returns** A string representing the whole URL from where our test starts
    
        """
        env = environment.lower()
        env_base_url = self.PROD_URL if env == 'prod' else base_url + urlSuffix
        return env_base_url

    def _get(self, locator):
        try:
            return self._wait.until(EC.visibility_of_element_located(locator))
        except TimeoutException:
            logging.info('\t[WebDriver Error] WebDriver timed out while trying to identify element '
                         'by {0!r}.'.format(locator))
            raise ElementDoesNotExistAssertionError(locator)

    def _gets(self, locator):
        try:
            return self._wait.until(EC.presence_of_all_elements_located(locator))
        except TimeoutException:
            logging.info(
                '\t[WebDriver Error] WebDriver timed out while trying to identify elements '
                'by {0!r}'.format(locator))
            raise ElementDoesNotExistAssertionError(locator)

    def _wait_for_element(self, element):
        self._wait.until(elementToBeClickable(element))

    def _is_link_valid(self, link):
        return self.__linkVerifier.is_link_valid(link.get_attribute('href'))

    def traverse_to_frame(self, frame):
        logging.info('\t[WebDriver] About to switch to frame {0!r}...'.format(frame))
        self._wait.until(EC.frame_to_be_available_and_switch_to_it(frame))
        logging.info('OK')

    def traverse_from_frame(self):
        logging.info('\t[WebDriver] About to switch to default content...')
        self._driver.switch_to.default_content()
        logging.info('OK')

    def set_timeout(self, new_timeout):
        self._driver.implicitly_wait(new_timeout)
        self._wait = WebDriverWait(self._driver, new_timeout)

    def restore_timeout(self):
        self._driver.implicitly_wait(wait_timeout)
        self._wait = WebDriverWait(self._driver, wait_timeout)

    def get_text(self, s):
        soup = BeautifulSoup(s)
        clean_out = soup.get_text()
        logging.info(clean_out)
        return clean_out

    def refresh(self):
        """
        refreshes the whole page
        """
        self._driver.refresh()
