#!/usr/bin/env python2

import requests, json, time, sys, pprint
from contentRepo import ContentRepo
from rhino import Rhino

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'

# example : PYTHONPATH=path/to/rhino/tools/python/plosapi python populateForAmbraTest.py http://localhost:8081 filestoretest.plos.org journal.pone.0004011

# TODO: complain about number of arguments

repoServer = sys.argv[1]
bucketName = sys.argv[2]
article = sys.argv[3]

dest = ContentRepo(repoServer)
source = Rhino()

# dest.createBucket(bucketName) # assume the bucket already exists

print ("article: " + article)

try:
	reps = source.assets(article)
except Exception as e:
	eLog.write("article: " + str(e) + "\n")
	# break

for (doi, representations) in reps.iteritems():

	for key in representations:

		if dest.objectExists(bucketName, key, '0'):
			print (key + "  already in repo, skipping upload")
			continue

		objectNoPrefix = source._stripPrefix(key, True)

		tempLocalFile = '/tmp/repo-pop-obj.temp'

		# TODO: add retry logic here
		try :

			objectData = source.getAfid(objectNoPrefix, tempLocalFile, 'MD5')

			contentType = objectData[2]	# requires rhino.py be updated to provide this info

			newObject = dest.newObject(bucketName, tempLocalFile, key, contentType, objectNoPrefix)

			uploadStatus = (newObject.status_code == requests.codes.created)

			print (key + "  uploaded: " + str(uploadStatus) )


		except Exception as e:
			print (key + "  request failed. skipping: " + str(e))
