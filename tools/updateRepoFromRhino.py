#!/usr/bin/env python2

"""
  Description: This script is used to check for new and updated articles in production
  that need to be moved to a content repo. It always reads from production Rhino.

  If you run this script with --testRun which will not update the repo.

  Input CSV data should be sent to stdin and output put should be sent to your desired CSV
  via stdout.
"""

from __future__ import print_function
from __future__ import with_statement
import sys
import os
from contentRepo import ContentRepo
from plosapi import Rhino
from plosapi.mkrepodb import decode_row

__author__    = 'Jono Finger'
__copyright__ = 'Copyright 2014, PLOS'
__version__   = '0.1'


def _clean_date_str(mod_date):
  mod_date = mod_date.replace('T', ' ')
  mod_date = mod_date.replace('Z', '')
  return mod_date

def diff_new(infile, args):
  """
  List the articles that have been added to Rhino using infile as the history

  Output should be redirected to a CSV file for example:
    cat ~/Desktop/article*.csv | PYTHONPATH=path/to/src/rhino/tools/python:path/to/content-repo/tools python updateRepoFromRhino.py --repoServer=http://localhost:8081 --repoBucket=org.plos.mybucket diffnew >> ~/Desktop/articles-new-5-28-2014.csv

  """

  repo = ContentRepo(args.repoServer)
  repoBucket = args.repoBucket

  if repoBucket == None:
    print('No bucket set', file=sys.stderr)
    return

  old = dict()
  for row in infile:
    (doi, ts, afid, md5, sha1, ct, sz, dname, fname) = decode_row(row)
    old['10.1371/'+doi] = ts
  rhino = Rhino()
  current = dict()
  for (doi, mod_date) in rhino.articles(lastModified=True):
    current[doi] = _clean_date_str(mod_date)

  i = 0
  for (doi, mod_date) in current.iteritems():
    if not old.has_key(doi):
      print(doi.replace('10.1371/', '') + " (%" + str(100*i/len(current)) + " done)", file=sys.stderr)

      if not _copy_from_rhino_to_repo(rhino, repo, repoBucket, doi, current[doi], args.testRun, 'new'):
        return
    i = i + 1

  return

def diff_mod(infile, args):
  """
  List the articles that have been modified in Rhino using infile as the history
  """

  repo = ContentRepo(args.repoServer)
  repoBucket = args.repoBucket

  if repoBucket == None:
    print('No bucket set', file=sys.stderr)
    return

  old = dict()
  for row in infile:
    (doi, ts, afid, md5, sha1, ct, sz, dname, fname) = decode_row(row)
    old['10.1371/'+doi] = ts
  rhino = Rhino()
  current = dict()
  for (doi, mod_date) in rhino.articles(lastModified=True):
    current[doi] = _clean_date_str(mod_date)

  i = 0
  for (doi, mod_date) in old.iteritems():
    if not current.has_key(doi):
      #print(doi + ' missing')
      continue
    elif not current[doi] == mod_date:
      print(doi.replace('10.1371/', '') + "  rhino=" + current[doi] +
            "  repo=" + mod_date + " (%" + str(100*i/len(current)) + " done)", file=sys.stderr)

      if not _copy_from_rhino_to_repo(rhino, repo, repoBucket, doi, current[doi], args.testRun, 'auto'):
        return
    i = i + 1

  return

def _copy_from_rhino_to_repo(rhino, repo, bucket, article, timestampStr, testRun, createMode = 'new'):

  # TODO: this is not technically correct because timestampStr is the article timestamp, not the asset timestamp

  rhino_reps = rhino.assets(article.replace('10.1371/', ''))

  for (rhino_asset_doi, representations) in rhino_reps.iteritems():

    for rhino_asset_key in representations:

      tempLocalFile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'repo-obj.temp')

      downloadAsset = rhino.getAfid(rhino_asset_key.replace('10.1371/', ''), tempLocalFile)

      if downloadAsset[5] != 'OK':
        print ("failed to download from Rhino " + rhino_asset_key, file=sys.stderr)
        return False

      if not testRun:
        uploadAsset = repo.uploadObject(bucket, tempLocalFile, rhino_asset_key, downloadAsset[3], rhino_asset_key, createMode, timestampStr)

        # for safety delete the downloaded local file
        os.remove(tempLocalFile)

        if uploadAsset.status_code != 201:
          print ("failed to upload to repo " + rhino_asset_key, file=sys.stderr)
          print (str(uploadAsset.status_code) + ": " + uploadAsset.text, file=sys.stderr)
          return False

      print('{doi}, {lm}, {afid}, {m}, {s}, {mt}, {sz}, csv-data'.format(
        doi=article.replace('10.1371/', ''), lm=timestampStr,
        afid=rhino_asset_key.replace('10.1371/', ''), m=downloadAsset[1], s=downloadAsset[2],
        mt=downloadAsset[3], sz=downloadAsset[4]))

  return True


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser(description='Push new articles from Rhino to a content repo')
  parser.add_argument('--repoServer', default='http://localhost:8081', help='Content repo server')
  parser.add_argument('--repoBucket', help='Content repo bucket')
  parser.add_argument('--testRun', default=False, action='store_true', help='Show the listing of changes but dont make them')
  parser.add_argument('command', help='Command', choices=['diffnew', 'diffmod'])
  #parser.add_argument('params', nargs='*', help="parameter list for commands")
  args = parser.parse_args()
  #params = args.params

  infile = sys.stdin

  # if args.file:
  #   infile = open(args.file, 'r')

  if args.testRun:
    print("TEST RUN! No data is being pushed.")

  if args.command == 'diffnew':
    diff_new(infile, args)
    sys.exit(0)

  if args.command == 'diffmod':
    diff_mod(infile, args)
    sys.exit(0)
