#!/usr/bin/env python2

import requests, json

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'


class ContentRepo:

	def __init__(self, repoServer='http://localhost:8080/'):

		self.repoServer = repoServer

	def listBuckets(self):
		url = self.repoServer + '/buckets/'
		r = requests.get(url)
		print (r.text)

	def createBucket(self, bucketName, bucketId=None):

		url = self.repoServer + '/buckets/'

		if bucketId == None:
			r = requests.post(url, data={'name' : bucketName})
		else:
			r = requests.post(url, data={'name' : bucketName, 'id': bucketId})

		return r.status_code == requests.codes.created

	def deleteBucket(self, bucketName):

		url = self.repoServer + '/buckets/'
		r = requests.delete(url + bucketName)

		return r.status_code == requests.codes.ok

	def newAsset(self, bucketName, fileLocation, key, contentType, downloadName):

		url = self.repoServer + '/assets/'

		files = {'file': open(fileLocation, 'rb')}
		values = {
			'key': key, 
			'bucketName' : bucketName, 
			'contentType' : contentType, 
			'downloadName' : downloadName,
			'newAsset' : 'true'
			}
		r = requests.post(url, files=files, data=values)

		return r
		#return r.status_code == requests.codes.created

	def _getAssetMetadataRequest(self, bucketName, key, versionNumber):
		url = self.repoServer + '/assets/' + bucketName

		values = {
			'key': key, 
			'version' : versionNumber,
			'fetchMetadata' : 'true'
			}

		r = requests.get(url, params=values)
		return r

	def assetExists(self, bucketName, key, versionNumber):
		return self._getAssetMetadataRequest(bucketName, key, versionNumber).status_code == requests.codes.ok
