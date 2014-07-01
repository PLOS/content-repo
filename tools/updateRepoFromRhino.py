#!/usr/bin/env python2

"""
  Description: This script is used to check for new and updated articles in production
  that need to be moved to a content repo. It always reads from production Rhino.

  If you run this script with --testRun which will not update the repo.

  Input CSV data should be sent to stdin and output put should be sent to your desired CSV
  via stdout.


README for updating a corpus repo from /mnt/corpus

	/mnt/corpus explained

		article-snapshot-2-complete.csv - the starting point for input rhino articles
		bill-data/	- the place to dump cached article directories after updateRempFromRhino dumps to an assetCache dir
		mysql-data/	- the directory that mysql should be using when its service starts
		old_filestore/	- a dir that can be used by the old ambra-file-store when looking for a filesystem implementation (sans MogileFS?)
		plos_repo/	- the directory where the repo stores its filesystem assets
		repubs/	- the list of republished files. we use this to generate an input list for updateRepoFromRhino
		update/	- this directory contains all the data that should added to article-snapshot-2-complete.csv and the cache that can be added to bill-data/


	To run the update script, check out rhino and content-repo to a local directory. Enter the content-repo/tools directory and pull the new articles like so:

		> cat /mnt/corpus/article-snapshot-2-complete.csv /mnt/corpus/update/articles-new-*.csv | PYTHONPATH=/home/jfinger/rhino/tools/python:/home/jfinger/content-repo/tools python updateRepoFromRhino.py --cacheDir=/mnt/corpus/update/articles-new-6-24-2014-1 --repoServer=http://localhost:8080 --repoBucket=corpus pushnew >> /mnt/corpus/update/articles-new-6-24-2014-1.csv


	To push repubs do the following:

		create the list of repubs you want to push. to get a full list:
		> ls -rt /mnt/corpus/repubs/ | sed -r 's/(p[a-z]+\.[0-9]+).*/journal.\1/' > /mnt/corpus/update/repubs.lst

		open the repubs.lst file and curate it down to the list by the dates you want to cover. to see the files listed by date do > ls -rtl /mnt/corpus/repubs/

		push the repubs:
			cat /mnt/corpus/update/repubs.lst | PYTHONPATH=/home/jfinger/rhino/tools/python:/home/jfinger/content-repo/tools python updateRepoFromRhino.py --cacheDir=/mnt/corpus/update/articles-repubs-6-24-2014-1 --repoServer=http://localhost:8080 --repoBucket=corpus pushrepubs >> /mnt/corpus/update/articles-repubs-6-24-2014-1.csv

"""

from __future__ import print_function
from __future__ import with_statement
import argparse
import sys
import os
import traceback
import hashlib
import datetime
from contentRepo import ContentRepo
from plosapi import Rhino

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'


def _handle_exception(key, e):
  print (key + str(e) + ", error")
  print (key + str(e), file=sys.stderr)
  print (traceback.format_exc(), file=sys.stderr)

def pushnew(infile, repo, skipSet, args):
  """
  List the articles that have been added to Rhino using infile as the history

  Output should be redirected to a CSV file for example:
    cat ~/Desktop/article*.csv | PYTHONPATH=path/to/src/rhino/tools/python:path/to/content-repo/tools python updateRepoFromRhino.py --cacheDir=assetCacheDir --repoServer=http://localhost:8081 --repoBucket=org.plos.mybucket pushnew >> ~/Desktop/articles-new-5-28-2014.csv

  """

  old = set()
  current = set()
  rhino = Rhino()

  for row in infile:

    try:
      cols = row.split(',')
      doi = cols[0]
      old.add(doi)
    except ValueError, e:
      pass
      #print("error parsing csv: " + str(e), file=sys.stderr)

  for (doi, mod_date) in rhino.articles(lastModified=True):
    if doi in skipSet:
      print ('skipping ' + doi + ' because in skipset', file=sys.stderr)
      continue

    current.add(doi.replace('10.1371/', ''))

  i = 0
  for doi in current:
    if not (doi in old):

      # NOTE: the reported percentage is calculated over the total articles in the corpus
      print(doi + ' (%' + str(100*i/len(current)) + ' done)', file=sys.stderr)

      try:
        _copy_from_rhino_to_repo(rhino, repo, args.repoBucket, doi, args.testRun, args.cacheDir, args.command)
      except Exception, e:
        _handle_exception(doi, e)

    i = i + 1

  return

def pushrepubs(infile, repo, args):
  """
  List the articles that have been modified in Rhino
    The input should be a list of articles that have been republished

  Protip: You can generate a list of repubs to be used as input for this like so:
  > ls -rt /mnt/corpus/repubs/ | sed -r 's/(p[a-z]+\.[0-9]+).*/journal.\1/'
  """

  i = 0
  rhino = Rhino()

  mods = []
  for doi in infile:
    mods.append(doi.rstrip())

  for doi in mods:

    print(doi + " (%" + str(100*i/len(mods)) + " done)", file=sys.stderr)

    try:
      _copy_from_rhino_to_repo(rhino, repo, args.repoBucket, doi, args.testRun, args.cacheDir, args.command)
    except Exception, e:
      _handle_exception(doi, e)

    i = i + 1

  return

def _copy_from_rhino_to_repo(rhino, repo, bucket, article, testRun, assetCache, operation):

  if operation == 'pushnew':
    createMode = 'new'


  articleFiles = rhino.articleFiles(article, assetCache)

  for (rhino_article_doi, assets) in articleFiles.iteritems():

    for (rhino_asset_key, (dlFname, dlMd5, dlSha1, dlContentType, dlSize, dlStatus)) in assets:

      rhino_asset_key_with_prefix = '10.1371/' + rhino_asset_key

      try:

        timestampStr = datetime.datetime.now().strftime('%Y-%m-%d %X')

        if dlStatus != 'OK':
          raise Exception('failed to download from Rhino ' + rhino_asset_key)

        if operation == 'pushrepubs': # if pushing repubs, dont push the same twice

          try:
            origObjMeta = repo.getObjectMetadata(bucket, rhino_asset_key_with_prefix)

            createMode = 'version'

            if origObjMeta['checksum'] == dlSha1:
              print ('skipping ' + rhino_asset_key + ' since checksum matches', file=sys.stderr)
              continue

          except LookupError:
            createMode = 'new'  # if the asset is not found in the repo, dont skip it

        if not testRun:
          uploadAsset = repo.uploadObject(bucket, os.path.join(assetCache, rhino_article_doi, dlFname), rhino_asset_key_with_prefix, dlContentType, rhino_asset_key_with_prefix, createMode, timestampStr)

          if uploadAsset.status_code != 201:
            raise Exception("failed to upload to repo " + rhino_asset_key + " , " + str(uploadAsset.status_code) + ": " + uploadAsset.text)

          # extra check to make sure the file made it to the server in tact
          if dlSha1 != hashlib.sha1(repo.getObjectData(bucket, rhino_asset_key_with_prefix)).hexdigest():
            raise Exception("the file download check failed for " + rhino_asset_key)


        print('{doi}, {lm}, {afid}, {m}, {s}, {mt}, {sz}, csv-data'.format(
          doi=article, lm=timestampStr,
          afid=rhino_asset_key, m=dlMd5, s=dlSha1, mt=dlContentType, sz=dlSize))

      except Exception, e:
        _handle_exception(rhino_asset_key, e)


if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Push new articles from Rhino to a content repo')
  parser.add_argument('--repoServer', required=True, help='Content repo server')
  parser.add_argument('--repoBucket', required=True, help='Content repo bucket')
  parser.add_argument('--cacheDir', required=True, help='Save files locally here as well as transfer to repo'),
  parser.add_argument('--testRun', default=False, action='store_true', help='Show the listing of changes but dont make them')
  parser.add_argument('command', help='Command', choices=['pushnew', 'pushrepubs'])
  #parser.add_argument('params', nargs='*', help="parameter list for commands")
  args = parser.parse_args()

  infile = sys.stdin

  repo = ContentRepo(args.repoServer)

  skipSet = set()
  skipSet.add('10.1371/annotation/33d82b59-59a3-4412-9853-e78e49af76b9')

  # make the directory absolute
  args.cacheDir = os.path.abspath(os.path.expanduser(args.cacheDir))

  if args.testRun:
    print('TEST RUN! No data is being pushed.', file=sys.stderr)

    if not repo.bucketExists(args.repoBucket):
      print('Bucket not found ' + args.repoBucket, file=sys.stderr)
      sys.exit(0)

  if args.command == 'pushnew':
    pushnew(infile, repo, skipSet, args)
    sys.exit(0)

  if args.command == 'pushrepubs':
    pushrepubs(infile, repo, args)
    sys.exit(0)
