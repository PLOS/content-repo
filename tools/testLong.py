#!/usr/bin/env python

# Copyright (c) 2017 Public Library of Science
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

#
# Content repo duration tester
#
# Instructions:
#   Install the repo python client (in soa-tools) or set your PYTHONPATH.
#   Make sure a test instance of the repo is up with an empty bucket.
#

import sys
import random
import time
import logging
from os import urandom
try:
  from cStringIO import StringIO
except:
  from StringIO import StringIO
import contentRepo

#--------------------------------------
# CONFIG
#--------------------------------------

# change this to base URL of the content-repo, e.g., "http://server:8081/repo"
base_url = 'http://localhost:8080'

# randomly pick a content type from this list
types = ['application/pdf', 'image/png', 'text/xml']

# randomly pick a size from this - (probability, min, max)
# with 0.5 probability, pick in range [1000, 5000], etc.
sizes = [
  (0.5, 1000, 5000),
  (0.4, 5000, 200000),
  (0.09, 200000, 1000000),
  (0.01, 1000000, 200000000)
]

# object key's prefix. change for each new test to avoid intefering with previous
key_prefix = 'test-2-object-'

# total number of iterations to run
iterations = 100000000
#iterations = 2

# wait after each iterations, in seconds
wait = 0.1

# print number of iterations completed every 100 iterations
show_on = 100

# logging level
loglevel = logging.WARNING

logger = logging.getLogger('test')

# number of bytes written so far
written = 0

#--------------------------------------
# EACH ITERATION: create/update, get
#--------------------------------------

# called repeatedly with increasing value of index.
# in each iteration, create a random data to upload and get
# also get one of the previously uploaded data
# raise RuntimeError if there is a problem
def run(index):
  global written
  # create a key to be used for the object
  key = '%s%09d'%(key_prefix, index,) # data key

  # random type and download file name
  type_ = random.choice(types)
  download = key + '.' + type_.split('/')[1]

  # random size from the sizes bucket
  size = random.randint(sizes[0][1], sizes[-1][2])
  rand = random.random()
  for r, m, n in sizes:
    if rand <= r:
      size = random.randint(m, n)
      break
    rand -= r

  # random content (binary) of this size
  data = urandom(size)

  logger.debug('[%d] key=%r size=%r type=%r', index, key, size, type_)

  # attempt to upload with "new" to create
  r = repo.uploadObject(bucket, StringIO(data), key, type_, download, "new")
  logger.debug('create status_code=%r', r.status_code)

  if r.status_code == 400 and r.content and r.content.startswith('Attempting to '):
    # create new failed, upload again with "version" to update
    r = repo.uploadObject(bucket, StringIO(data), key, type_, download, "version")

  if r.status_code != 200 and r.status_code != 201:
    # failed to create or update
    raise RuntimeWarning('upload failed key=%r status=%r body=%r'%(key, r.status_code, r.content))

  written += size

  # get the meta data of just uploaded object
  try:
    metadata = repo.getObjectMetadata(bucket, key)
    logger.debug('[%d] get-metadata %r', index, metadata)
  except:
    raise RuntimeWarning('get-metadata failed key=%r error=%r'%(key, sys.exc_info()[1]))

  # get the content of just uploaded object
  try:
    content = repo.getObjectData(bucket, key)
  except:
    raise RuntimeWarning('get failed key=%r error=%r'%(key, sys.exc_info()[1]))


  # verify the size is correct
  if len(content) != size:
    raise RuntimeWarning('get returned wrong size %r!=%r'%(content and len(content), size))

  # randomly try get on a previous key
  key = '%s%09d'%(key_prefix, random.randint(1, index),) # data key

  try:
    metadata = repo.getObjectMetadata(bucket, key)
    logger.debug('[%d] alt get-metadata %r', index, metadata)
  except:
    raise RuntimeWarning('get-alt-metadata failed key=%r error=%r'%(key, sys.exc_info()[1]))

  try:
    content = repo.getObjectData(bucket, key)
  except:
    raise RuntimeWarning('get-alt failed key=%r error=%r'%(key, sys.exc_info()[1]))


#--------------------------------------
# MAIN
#--------------------------------------

if __name__ == '__main__':
  # configure logging
  logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=loglevel)

  # check the bucket name, use the first one available
  repo = contentRepo.ContentRepo(base_url)
  bucket = repo.listBuckets()[0]
  logger.info('bucket=%r', bucket)

  # loop through each iteration
  index = 0
  nextwait = wait
  while True:
    # wait between iterations
    time.sleep(nextwait)
    index += 1
    if index > iterations:
      break
    if index % show_on == 0:
      logger.warning('iterations %d - written %d', index, written)

    try:
      run(index)
      nextwait = wait
    except RuntimeWarning, w:
      logger.warning('[%d] %s', index, w.message)
      nextwait = min(60, nextwait*2)
      logger.warning('changed nextwait to %r', nextwait)
    except:
      logger.exception('[%d] exception', index)
      nextwait = min(60, nextwait*2)
      logger.warning('changed nextwait to %r', nextwait)
