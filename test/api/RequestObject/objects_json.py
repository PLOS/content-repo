#!/usr/bin/env python2

"""
Base class for CREPO objects JSON related services
"""

__author__ = 'msingh@plos.org'

from ...Base.base_service_test import BaseServiceTest
from ...Base.Config import API_BASE_URL
from ...Base.api import needs
from ...Base.MySQL import MySQL
from buckets_json import DEFAULT_HEADERS

OBJECTS_API = API_BASE_URL + '/objects'
CREPO_DB = 'PLOS_REPO'

# Http Codes
OK = 200
CREATED = 201
BAD_REQUEST = 400
NOT_FOUND = 404
NOT_ALLOWED = 405


class ObjectsJson(BaseServiceTest):

  def get_objects(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to get objects list in a bucket
    GET /objects?bucketName={bucketName}...

    :param bucketName, offset, limit, includeDeleted, includePurged, tag
    """
    path = '%s?bucketName=%s' % (OBJECTS_API, bucketName) if bucketName else '%s' % (OBJECTS_API,)
    self.doGet(path, kwargs, DEFAULT_HEADERS)
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
    path = '%s/versions/%s' % (OBJECTS_API, bucketName) if bucketName else '%s' % (OBJECTS_API)
    self.doGet(path, params=kwargs, headers=DEFAULT_HEADERS)
    self.parse_response_as_json()

  def get_object_meta(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to get a object meta
    :param name: bucket name.
    """
    path = '%s/meta/%s' % (OBJECTS_API, bucketName) if bucketName else '%s/meta' % (OBJECTS_API,)
    self.doGet(path, params=kwargs, headers=DEFAULT_HEADERS)
    self.parse_response_as_json()

  def delete_object(self, bucketName=None, **kwargs):
    """
    Calls CREPO API to delete a object
    :param name: bucket name.
    """
    path = '%s/%s' % (OBJECTS_API, bucketName) if bucketName else '%s' % (OBJECTS_API,)
    self.doDelete(path, params=kwargs, headers=DEFAULT_HEADERS)

  """
  Below SQL statements will query ambra syndication table given archiveName
  """
  def get_test_objects_sql (self, bucketName):
    objects = MySQL().query('SELECT o.objkey, o.uuid '
                                 'FROM '+CREPO_DB+'.objects o '
                                 'JOIN '+CREPO_DB+'.buckets b  ON b.bucketId = o.bucketId '
                                 'WHERE b.bucketName = %s '
                                 'AND o.objkey like \'%testobject%\' '
                                 'AND o.status = 0 '
                                 'ORDER BY o.timestamp', [bucketName])
    return objects

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
