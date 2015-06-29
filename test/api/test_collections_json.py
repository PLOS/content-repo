#!/usr/bin/env python2

__author__ = 'gfilomeno@plos.org'

"""
POST /collections
Create a new collection. Create a new version of a collection.
Fail to create new version if collection does not exist. or bucket does not exist or object does not exist.

GET /collections
List collections in a bucket. Limit and offset for result. Include deleted or purged.
Fail to get if bucket does not exist.

GET /collections/{bucketName} ?key
Get single collection by key or its metadata.

GET /collections/versions/{bucketName} ?key
Get all the versions of a collection.

DELETE /collections/{bucketName} ?key
Delete a collection.
"""
from ..api.RequestObject.collections_json import CollectionsJson, OK, CREATED, BAD_REQUEST, NOT_FOUND, \
  KEY_NOT_ENTERED, COLLECTION_NOT_FOUND, BUCKET_NOT_FOUND, FILTER_NOT_ENTERED
from ..api.RequestObject.buckets_json import BucketsJson
import requests
import random
import StringIO
import time
import uuid


class TestCollections(CollectionsJson):

  bucketName = BucketsJson.get_bucket_name()

  def setUp(self):
    self.already_done = 0
    self.objKey = None
    self.collKeys = []

  def tearDown(self):
    """
    Purge all objects and collections created in the test case
    """
    if self.already_done > 0: return
    # Delete collection's object for test
    self.delete_test_object()
    # Delete collections for test
    self.delete_test_collection()

  def test_post_collections_new(self):
    """
    Create a new collection.
    """
    print('\nTesting POST /collections\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # verify the collection version number
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey())
    self.verify_http_code_is(OK)
    self.verify_get_collection(key=self.parsed.get_collectionKey()[0], versionNumber=0)

  def test_post_collections_version(self):
    """
    Create a new version of a collection.
    """
    print('\nTesting POST /collections create a new version\n')
    # Create a new collection
    key = TestCollections.get_collection_key()
    self.post_collections(self.create_collection_request(key=key, create='auto'))
    self.verify_http_code_is(CREATED)
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey())
    version = self.parsed.get_collectionVersionNumber()[0]
    time.sleep(1)  # this is needed, otherwise the second POST does not work. TODO: file a bug.
    # Create a version collection
    self.post_collections(self.create_collection_request(key=key, objects=self.get_object_json(), create='version'))
    self.verify_http_code_is(CREATED)
    self.get_collection(self.bucketName, key=key)
    self.verify_get_collection(key=key, versionNumber=version + 1)
    self.get_collection_versions(self.bucketName, key=key)
    collection_list = self.parsed.get_collections()
    self.assertEquals(2, len(collection_list), 'there are not all collection versions')

  def test_post_collection_no_bucket(self):
    """
    Fail to post objects if no bucket
    """
    print('\nTesting POST /collections without bucket\n')
    bucket_name = 'testbucket%d' % random.randint(1000, 1999)
    self.post_collections(self.create_collection_request(bucketName=bucket_name, create='new'))
    self.verify_http_code_is(NOT_FOUND)

  def test_post_collection_no_object(self):
    """
    Fail to post objects if no bucket
    """
    print('\nTesting POST /collections without object \n')
    objects = [{'key': 'testobject', 'uuid': '604b8984-cf9f-4c3c-944e-d136d53770da'}]
    self.post_collections(self.create_collection_request(create='new', objects=objects))
    self.verify_http_code_is(NOT_FOUND)

  def test_post_collections_version_not_exist(self):
    """
    Fail to post objects version if not exist
    """
    print('\nTesting POST /collections create version when collection does not exist\n')
    collKey = TestCollections.get_collection_key()
    self.get_collection(bucketName=self.bucketName, key=collKey )
    self.verify_http_code_is(NOT_FOUND)
    self.post_collections(self.create_collection_request(key=collKey, create='version'))
    self.verify_http_code_is(BAD_REQUEST)

  """
  DELETE /collections
  """
  def test_delete_collection(self):
    """
    Delete a new collection
    """
    print('\nTesting DELETE /collections\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    collection = self.parsed.get_json()
    # Delete the new collection
    self.delete_collection(bucketName=self.bucketName, key=collection['key'], uuid=collection['uuid'])
    self.verify_http_code_is(OK)
    # Validate the new collection was deleted
    self.get_collection(bucketName=self.bucketName, key=collection['key'], uuid=collection['uuid'])
    self.verify_http_code_is(NOT_FOUND)

  def test_delete_collection_without_filter(self):
    """
    Try to delete a new collection without filter value
    """
    print('\nTesting DELETE /collections without filter\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    # Delete the new collection
    self.delete_collection(bucketName=self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(BAD_REQUEST)
    self.verify_message_text(FILTER_NOT_ENTERED)

  def test_delete_collection_invalid_bucket(self):
    """
    Try to delete a new collection with a invalid bucket value
    """
    print('\nTesting DELETE /collections invalid bucket name\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    collection = self.parsed.get_json()
    # Delete the new collection
    try:
      self.delete_collection(bucketName='@9%!#d', key=collection['key'], uuid=collection['uuid'])
      self.fail('No JSON object could be decoded')
    except:
      pass

  def test_delete_collection_invalid_key(self):
    """
    Try to delete a new collection with a invalid key value
    """
    print('\nTesting DELETE /collections invalid key\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    collection = self.parsed.get_json()
    # Delete the new collection
    self.delete_collection(bucketName=self.bucketName, key='@9%!#d',
                           uuid=collection['uuid'])
    self.verify_http_code_is(NOT_FOUND)

  def test_delete_collection_invalid_params(self):
    """
    Try to delete a new collection with a invalid parameters
    """
    print('\nTesting DELETE /collections invalid params\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Delete the new collection sending invalid parameters
    try:
      self.delete_collection(bucketName='@9%!#d', key='@9%!#d', uuid='@9%!#d')
      self.fail('No JSON object could be decoded')
    except:
      pass

  def test_delete_collection_only_bucket(self):
    """
    Try to delete a new collection with only the bucket
    """
    print('\nTesting DELETE /collections only bucket\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    collection = self.parsed.get_json()
    # Delete the new collection
    self.delete_collection(bucketName=self.bucketName)
    self.verify_http_code_is(BAD_REQUEST)
    self.verify_message_text(KEY_NOT_ENTERED)

  def test_delete_collection_only_key(self):
    """
    Try to delete a new collection with only the key
    """
    print('\nTesting DELETE /collections only key\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    # Delete the new collection without bucket return a HTML 405 - Method Not Allowed
    try:
      self.delete_collection(bucketName='', key=self.parsed.get_collectionKey()[0])
      self.fail('No JSON object could be decoded')
    except:
      pass

  def test_delete_collection_only_filter(self):
    """
    Try to delete a new collection with only the key
    """
    print('\nTesting DELETE /collections only filter\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Get collection uuid
    self.get_collection(self.bucketName, key=self.parsed.get_collectionKey()[0])
    self.verify_http_code_is(OK)
    # Delete the new collection without bucket return a HTML 405 - Method Not Allowed
    try:
      self.delete_collection(bucketName='',uuid=self.parsed.get_collectionUUID()[0])
      self.fail('No JSON object could be decoded')
    except:
      pass

  def test_delete_collection_without_params(self):
    """
    Try to delete a new collection without parameters
    """
    print('\nTesting DELETE /collections without params\n')
    # Create a new collection
    self.post_collections(self.create_collection_request(create='new'))
    self.verify_http_code_is(CREATED)
    # Delete the new collection without bucket return HTML 405 - Not method allowed
    try:
      self.delete_collection(bucketName='')
      self.fail('No JSON object could be decoded')
    except:
      pass

  """
  GET /collections/ list
  """
  def test_get_collections(self):
    """
    Calls CREPO API to GET /collections list
    """
    print('\nTesting List collections (GET)\n')
    self.get_collections(self.bucketName)
    self.verify_get_collection_list(1000)

  def test_get_collections_limit_only(self):
    """
    Calls CREPO API to GET /collections list with a limit
    """
    print('\nTesting List collections (GET) with limit only\n')
    limit = '%d' % random.randint(1, 1000)
    self.get_collections(self.bucketName, limit=limit)
    self.verify_get_collection_list(limit)

  def test_get_collections_without_bucket(self):
    """
    Calls CREPO API to GET /collections list without bucket name
    """
    print('\nTesting List collections (GET) without bucket\n')
    self.get_collections(bucketName=None)
    self.verify_http_code_is(NOT_FOUND)
    self.verify_message_text(BUCKET_NOT_FOUND)

  def test_get_collections_invalid_bucket(self):
    """
    Calls CREPO API to GET /collections list with a invalid bucket
    """
    print('\nTesting List collections (GET) with a invalid bucket\n')
    self.get_collections(bucketName='@9%&!d#')
    self.verify_http_code_is(NOT_FOUND)
    self.verify_message_text(BUCKET_NOT_FOUND)

  """
  GET /collections/{bucketName}
  """
  def test_get_collection(self):
    """
    Calls CREPO API to GET /collections/{bucketName}
    """
    print('\nTesting GET collections/{bucketName}\n')
    try:
      collection = self.get_first_collection()
      self.get_collection(bucketName=self.bucketName, key=collection['key'])
      self.verify_get_collection(key=collection['key'], status='USED')
    except ValueError as err:
      print err

  def test_get_collection_bucket_only(self):
    """
    Get collections API call with bucket only
    """
    print('\nTesting GET collections/{bucketName} with bucketName only\n')
    self.get_collection(bucketName=self.bucketName)
    self.verify_http_code_is(BAD_REQUEST)
    self.verify_message_text(KEY_NOT_ENTERED)

  def test_get_collection_key_only(self):
    """
    Get collections API call with key only
    """
    print('\nTesting GET collections/{bucketName} with key only\n')
    try:
      collection = self.get_first_collection()
      self.get_collection(key=collection['key'])
      self.verify_http_code_is(NOT_FOUND)
      self.verify_message_text(COLLECTION_NOT_FOUND)
    except ValueError as err:
      print err

  def test_get_collection_no_parameters(self):
    """
    Calls CREPO API to get collection without parameters
    """
    print('\nTesting GET collections/{bucketName} without parameters\n')
    self.get_collection(bucketName=None)
    self.verify_http_code_is(BAD_REQUEST)
    self.verify_message_text(KEY_NOT_ENTERED)

  def test_get_collection_invalid_key(self):
    """
    Get collections API call with a invalid key
    """
    print('\nTesting GET collections/{bucketName} with a invalid key\n')
    self.get_collection(bucketName=self.bucketName, key='@9%&!d#')
    self.verify_http_code_is(NOT_FOUND)
    self.verify_message_text(COLLECTION_NOT_FOUND)

  def test_get_collection_invalid_bucket(self):
    """
    Get collections API call with a invalid bucket
    """
    print('\nTesting GET collections/{bucketName} with a invalid bucket name\n')
    try:
      collection = self.get_first_collection()
      self.get_collection(bucketName='@9%&!d#', key=collection['key'])
      self.verify_http_code_is(NOT_FOUND)
      self.verify_message_text(BUCKET_NOT_FOUND)
    except ValueError as err:
      print err

  def test_get_collection_invalid_parameters(self):
    """
    Get collections API call with invalid parameters
    """
    print('\nTesting GET collections/{bucketName} with invalid parameters\n')
    try:
      self.get_collection(bucketName='@9%&!d#', key='@9%&!d#')
    except ValueError as e:
      print e

  def get_first_collection(self):
    """
    Calls CREPO API to GET /collections/ list and return the first collection
    """
    self.get_collections(self.bucketName, limit=1)
    self.verify_http_code_is(OK)
    if self.parsed.get_collections() and len(self.parsed.get_collections()) > 0:
      return self.parsed.get_collections()[0]
    else:
      raise ValueError('\nThere are not collections\n')

  def create_collection_request(self, **params):
    if params.has_key('key'):
      key = params['key']
    else:
      key = TestCollections.get_collection_key()
    if params.has_key('bucketName'):
      bucket = params['bucketName']
    else:
      bucket = self.bucketName
    if params.has_key('objects'):
      objects = params['objects']
    else:
      objects = self.post_new_object()
    self.collKeys.append(key)
    user_metadata = TestCollections.get_usermetada(key)
    return {'bucketName': bucket, 'key': key,
            'create': params['create'], 'objects': objects, 'userMetadata': user_metadata}

  def get_object_json(self):
    # Get JSON objects
    self.get_object_meta(bucketName=self.bucketName, key=self.objKey)
    return {'key': self.objKey, 'uuid': self.parsed.get_objectAttribute('uuid')}

  def post_new_object(self):
    self.objKey = self.get_object_key()
    download = '%s.txt' % (self.objKey,)
    self.post_object(bucketName=self.bucketName, key=self.objKey,
                     contentType='text/plain', downloadName=download,
                     create='auto', files=[('file', StringIO.StringIO('test content - ' + self.objKey))])
    self.verify_http_code_is(CREATED)
    return self.get_object_json()

  def delete_test_object(self):
    if self.objKey:
      self.delete_object(bucketName=self.bucketName, key=self.objKey, version=0,  purge=True)
      self.verify_http_code_is(OK)


  def verify_status_test_object(self):
    self.get_object_meta(bucketName=self.bucketName,  key=self.objKey, version=0)
    print '\nVerify if the object was deleted. HTTP status code: ' + str(self.get_http_response().status_code)
    return self.get_http_response().status_code == NOT_FOUND

  def delete_test_collection(self):
    for key in self.collKeys:
      self.get_collection_versions(bucketName=self.bucketName, key=key)
      collections = self.parsed.get_collections()
      if collections:
        for coll in collections:
          self.delete_collection(bucketName=self.bucketName, key=coll['key'], version=coll['versionNumber'])
          self.verify_http_code_is(OK)

  @staticmethod
  def get_collection_key():
    collUUID = uuid.uuid4()
    return 'testcollection%s' % str(collUUID).translate(None, "',-")

  @staticmethod
  def get_object_key():
    objUUID = uuid.uuid4()
    return 'testobject%s' % str(objUUID).translate(None, "',-")

  @staticmethod
  def get_usermetada(key):
    return {'path': '/crepo/mogile/%r' % key}


if __name__ == '__main__':
    CollectionsJson._run_tests_randomly()
