#!/usr/bin/env python2

import requests
import json
import argparse
import sys

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'


class ContentRepo:

  def __init__(self, repoServer='http://localhost:8080/'):
    self.repoServer = repoServer

    # ping server to make sure it is up, will throw an exception if not found
    requests.get(repoServer + "/buckets")

  def listBuckets(self):

    # TODO: specify Accept: application/json

    url = self.repoServer + '/buckets/'
    r = requests.get(url)
    jsonData = json.loads(r.text)

    bucketList = []

    for b in jsonData:
      bucketList.append(b['bucketName'])

    return (bucketList)

  def bucketExists(self, bucketName):
    bucketList = self.listBuckets()

    for b in bucketList:
      if bucketName == b:
        return True

    return False

  def createBucket(self, bucketName, bucketId=None):

    url = self.repoServer + '/buckets/'

    if bucketId == None:
      r = requests.post(url, data={'name': bucketName})
    else:
      r = requests.post(url, data={'name': bucketName, 'id': bucketId})

    return r.status_code == requests.codes.created

  def deleteBucket(self, bucketName):

    url = self.repoServer + '/buckets/'
    r = requests.delete(url + bucketName)

    return r.status_code == requests.codes.ok

  def uploadObject(self, bucketName, fileLocation, key, contentType, downloadName, create, timestampStr=None):

    # if timestamp == None:
    #   timestampStr = None
    # else:
    #   timestampStr = timestamp.strftime('%Y-%m-%d %X')  # yyyy-[m]m-[d]d hh:mm:ss[.f...]

    url = self.repoServer + '/objects/'

    files = {'file': open(fileLocation, 'rb')}
    values = {
      'key': key, 
      'bucketName': bucketName,
      'contentType': contentType,
      'downloadName': downloadName,
      'timestamp': timestampStr,
      'create': create
    }

    r = requests.post(url, files=files, data=values)

    return r

  def _getObjectMetadataRequest(self, bucketName, key, versionNumber):
    url = self.repoServer + '/objects/' + bucketName

    values = {
      'key': key, 
      'version': versionNumber,
      'fetchMetadata': 'true'
    }

    r = requests.get(url, params=values)
    return r

  def getObjectData(self, bucketName, key, versionNumber=None):
    """
    Get the object data
    Note: This only works with filestores, since it does not yet deal with reproxying
    """

    url = self.repoServer + '/objects/' + bucketName

    values = {
      'key': key
    }

    if not (versionNumber is None):
      values['version'] = versionNumber

    r = requests.get(url, params=values)
    return r.content

  def objectExists(self, bucketName, key, versionNumber):
    return self._getObjectMetadataRequest(bucketName, key, versionNumber).status_code == requests.codes.ok


# The following section is a work in progress

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Command line interface to content repo')
  parser.add_argument('--server', default='http://localhost:8081', help='Content repo server')
  parser.add_argument('--bucket', help='Content repo bucket')
  parser.add_argument('--key', help='Object key')
  parser.add_argument('--version', help='Object version')
  parser.add_argument('command', help='Command', 
    choices=['listBuckets', 'deleteBucket', 'objectInfo', 'getObject', 'newObject'])

  #parser.add_argument('params', nargs='*', help="parameter list for commands")
  args = parser.parse_args()

  infile = sys.stdin

  repo = ContentRepo(args.server)

  if args.command == 'listBuckets':
    buckets = repo.listBuckets()
    for b in buckets:
      print(b)

  if args.command == 'deleteBucket':
    result = repo.deleteBucket(args.bucket)
    print (result)

  if args.command == 'objectInfo':
    response = repo._getObjectMetadataRequest(args.bucket, args.key, args.version)
    print (response.text)

  if args.command == 'getObject':
    data = repo.getObjectData(args.bucket, args.key, args.version)
    print (data)

  # if args.command == 'newObject':
  #   response = repo.uploadObject(args.bucket, infile, args.key, None, args.key, 'auto')
  #   print (response.text)
