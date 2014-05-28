#!/usr/bin/env python2

import requests
import json
import os
import time
import sys
import argparse
import pprint
from contentRepo import ContentRepo
from rhino import Rhino

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'

# exec with : PYTHONPATH=path/to/rhino/tools/python/plosapi python rhinoToRepo.py -c http://localhost:8084 -a 100


argparser = argparse.ArgumentParser(description='Migrate assets from a production Rhino server to a content repo server.')
argparser.add_argument('--repoServer', '-c', default='http://localhost:8082', help='Content repo server')
argparser.add_argument('--rhinoServer', '-r', required=False, help='Rhino server')
argparser.add_argument('--startAtArticle', '-a', required=False, default=0, type=int, help='Start at article number (order is not preserved, so this represents a crude guess)')

args = argparser.parse_args()


rhinoToRepoLog = 'rhinoToRepo.log'
errorLog = 'error.log'
logMessage = open(rhinoToRepoLog, 'a')
logError = open(errorLog, 'a')

dest = ContentRepo(args.repoServer)

tempLocalFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'repo-obj.temp')

if args.rhinoServer is None:
    source = Rhino()
else:
    source = Rhino(rhinoServer=args.rhinoServer, rver="") # this is for referencing a local rhino instance

bucketName = 'us-west-1.repo.plos.org'

dest.createBucket(bucketName)

articles = source.articles(False, True)

a = 0  # article count
for article in articles:

    if a < args.startAtArticle:
        a += 1
        continue

    print ('article (' + str(a) + '): ' + article)

    try:
        reps = source.assets(article)
    except Exception as e:
        logError.write('article (' + str(a) + '): ' + str(e) + '\n')
        continue

    for (doi, representations) in reps.iteritems():

        for key in representations:

            if dest.objectExists(bucketName, key, '0'):
                print (key + '  already in repo, skipping upload')
                continue

            objectNoPrefix = source._stripPrefix(key, True)

            #tempLocalFile = str(uuid.uuid4())+'.tmp'

            # TODO: add retry logic here
            try:

                begin = time.time()
                _, checksumMD5, checksumSHA1, contentType, sourceSize = source.getAfid(objectNoPrefix, tempLocalFile)
                readTime = time.time() - begin

                begin = time.time()
                newObject = dest.uploadObject(bucketName, tempLocalFile, key, contentType, objectNoPrefix, 'new')
                writeTime = time.time() - begin

                objectJson = json.loads(newObject.text)
                objectJson['md5'] = checksumMD5
                objectJson['a'] = a
                uploadStatus = (newObject.status_code == requests.codes.created)

                print ('(a ' + str(a) + ') ' + key + '  uploaded: ' + str(uploadStatus) + '  size: ' + str(objectJson[u'size']) + '  read: {0:.2f}'.format(round(readTime, 2)) + '  write: {0:.2f}'.format(round(writeTime, 2)))

                logMessage.write(pprint.pformat(objectJson) + '\n')

                if (not objectJson[u'size'] == sourceSize) or (not objectJson[u'checksum'] == unicode(checksumSHA1)):
                    print ('Input and output files do not match')
                    print ('input checksum ' + checksumSHA1)
                    pprint.pprint(objectJson)
                    sys.exit(-2)

                if objectJson[u'size'] is 0:
                    print ('Warning: File size = 0')
                    sys.exit(-1)

                #os.remove(tempLocalFile)

            except Exception as e:
                print (key + '  request failed. skipping: ' + str(e))

    a += 1
