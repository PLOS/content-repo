#!/usr/bin/env python2

import requests, json
from contentRepo import ContentRepo
from rhino import Rhino

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'

# exec with : PYTHONPATH=/home/jfinger/src/rhino/tools/python/plosapi python rhinoToRepo.py

md5listFile = 'md5list.csv'
f = open(md5listFile, 'a')

dest = ContentRepo()
source = Rhino()

bucketName = 'plos-rhino-migration'

dest.createBucket(bucketName)

articles = source.articles(False, True)

a = 0
for article in articles:

	print ("article (" + str(a) + "): " + article)

	reps = source.assets(article)

	for (doi, representations) in reps.iteritems():

		for key in representations:

			if dest.assetExists(bucketName, key, '0'):
				print (key + "  already in repo, skipping upload")
				continue

			assetNoPrefix = source._stripPrefix(key, True)

			tempLocalFile = '/tmp/temp.asset'

			assetData = source.getAfid(assetNoPrefix, tempLocalFile, 'MD5')

			checksumMD5 = assetData[1]
			contentType = assetData[2]	# requires rhino.py be updated to provide this info

			upload = dest.newAsset(bucketName, tempLocalFile, key, contentType, assetNoPrefix)

			print (key + "  uploaded: " + str(upload))

			f.write(key + "\t" + checksumMD5 + "\n")

	a += 1

	if a == 6:
		break
