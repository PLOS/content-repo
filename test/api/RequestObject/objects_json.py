#!/usr/bin/env python2

"""
Base class for CREPO objects JSON related services
"""

__author__ = 'msingh@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from buckets_json import DEFAULT_HEADERS

OBJECTS_API = API_BASE_URL + '/objects'

# Http Codes
OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404


class ObjectsJson(BaseServiceTest):

  def get_objects(self, **kwargs):
    """
    Calls CREPO API to get objects list in a bucket
    GET /objects ?bucketName={bucketName}...

    :param bucketName, offset, limit, includeDeleted, includePurged, tag
    """
    self.doGet('%s' % OBJECTS_API, kwargs, DEFAULT_HEADERS)
    self.parse_response_as_json()

  def post_objects(self, files=None, **kwargs):
    """
    Calls CREPO API to create object.

    :param bucketName, key, contentType, downloadName, create, userMetadata, file, ...
    """
    self.doPost('%s' % OBJECTS_API, data=kwargs, files=files, headers=DEFAULT_HEADERS)
    self.parse_response_as_json()

  def get_object(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to get a object
    :param name: bucket name.
    """
    self.doGet('%s/%s' % (OBJECTS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)
    if kwargs.get('fetchMetadata', False):
      self.parse_response_as_json()

  def get_object_versions(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to get a object versions
    :param name: bucket name.
    """
    self.doGet('%s/versions/%s' % (OBJECTS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)
    self.parse_response_as_json()

  def get_object_meta(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to get a object meta
    :param name: bucket name.
    """
    self.doGet('%s/meta/%s' % (OBJECTS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)
    self.parse_response_as_json()

  def delete_object(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to delete a object
    :param name: bucket name.
    """
    self.doDelete('%s/%s' % (OBJECTS_API, bucketName), params=kwargs, headers=DEFAULT_HEADERS)

  @needs('parsed', 'parse_response_as_json()')
  def verify_get_objects(self):
    """
    Verifies a valid response for GET /objects.
    """
    self.verify_http_code_is(OK)

  @needs('parsed', 'parse_response_as_json()')
  def verify_get_object_meta(self, **kwargs):
    """
    Verifies a valid response for GET /objects/meta/{bucketName}
    """
    self.verify_http_code_is(OK)
    for k, v in kwargs.items():
      actual = self.parsed.get_objectAttribute(k)
      self.assertEquals(actual, v, '%r is not correct: %r != %r' % (k, v, actual))

  def verify_get_object(self, content=''):
    """
    Verifies a valid response for GET /objects/{bucketName}
    """
    self.assertEquals(self.get_http_response().text, content, 'content %r != %r' % (content,
                                                                                    self.get_http_response().text))
