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
"""

__author__ = 'jkrzemien@plos.org'

from abc import ABCMeta, abstractmethod


class AbstractResponse(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_buckets(self):
        pass

    @abstractmethod
    def get_bucketID(self):
        pass

    @abstractmethod
    def get_bucketName(self):
        pass

    @abstractmethod
    def get_bucketTimestamp(self):
        pass

    @abstractmethod
    def get_bucketCreationDate(self):
        pass

    @abstractmethod
    def get_bucketActiveObjects(self):
        pass

    @abstractmethod
    def get_bucketTotalObjects(self):
        pass

    @abstractmethod
    def get_objectKey(self):
        pass

    @abstractmethod
    def get_collections(self):
        pass

    @abstractmethod
    def get_collectionKey(self):
        pass

    @abstractmethod
    def get_collectionVersionNumber(self):
        pass

    @abstractmethod
    def get_collectionUUID(self):
        pass

    @abstractmethod
    def get_collectionStatus(self):
        pass

    @abstractmethod
    def get_repoErrorCode(self):
        pass

    @abstractmethod
    def get_message(self):
        pass
