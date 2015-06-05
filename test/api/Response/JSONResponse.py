#!/usr/bin/env python2

"""
Base class for Rhino's JSON based service tests.
Python's JSONPath can be installed via the following command:
  sudo pip install --allow-external jsonpath --allow-unverified jsonpath jsonpath
"""

__author__ = 'jgray@plos.org'

import json
from jsonpath import jsonpath
from AbstractResponse import AbstractResponse


class JSONResponse(AbstractResponse):

  _json = None

  def __init__(self, response):
    try:
      self._json = json.loads(response)
    except Exception as e:
      print 'Error while trying to parse response as JSON!'
      print 'Actual response was: "%s"' % response
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

  def get_hasXReproxy(self):
    return self.jpath('*')

