#!/usr/bin/env python2

import requests, json, time, sys, pprint
from contentRepo import ContentRepo
from rhino import Rhino

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'

# exec with : PYTHONPATH=path/to/rhino/tools/python/plosapi python rhinoToRepo.py 100

startArticleNum = int(sys.argv[1])

if startArticleNum == None:
	startArticleNum = 0

rhinoToRepoLog = 'rhinoToRepo.log'
f = open(rhinoToRepoLog, 'a')

dest = ContentRepo()
source = Rhino()

bucketName = 'us-west-1.repo.plos.org'

dest.createBucket(bucketName)

articles = source.articles(False, True)

a = 0
for article in articles:

	if a < startArticleNum:
		a += 1
		continue

	print ("article (" + str(a) + "): " + article)

	reps = source.assets(article)

	for (doi, representations) in reps.iteritems():

		for key in representations:

			if dest.assetExists(bucketName, key, '0'):
				print (key + "  already in repo, skipping upload")
				continue

			assetNoPrefix = source._stripPrefix(key, True)

			tempLocalFile = '/tmp/temp.asset'

			# TODO: add retry logic here
			try :

				begin = time.time()
				assetData = source.getAfid(assetNoPrefix, tempLocalFile, 'MD5')
				readtime = time.time() - begin

				checksumMD5 = assetData[1]
				contentType = assetData[2]	# requires rhino.py be updated to provide this info

				begin = time.time()
				newAsset = dest.newAsset(bucketName, tempLocalFile, key, contentType, assetNoPrefix)
				writetime = time.time() - begin

				assetJson = json.loads(newAsset.text)
				assetJson["md5"] = checksumMD5
				uploadStatus = (newAsset.status_code == requests.codes.created)

				print ("(a " + str(a) + ") " + key + "  uploaded: " + str(uploadStatus) + "  read: {0:.2f}".format(round(readtime, 2)) + "  write: {0:.2f}".format(round(writetime, 2)))

#				f.write(key + "\t" + checksumMD5 + "\t" + contentType + "\n")
				f.write (pprint.pformat(assetJson) + "\n")


			except Exception as e:
				print (key + "  request failed. skipping: " + str(e))

	a += 1

