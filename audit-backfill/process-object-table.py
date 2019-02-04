#!/usr/bin/env python

# Copyright (c) 2014-2019 Public Library of Science
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

import os, sys

def parse_line(l):
  return l.strip().replace('"', '').split(',')

STAT_USED = '0'
STAT_DEL = '1'
STAT_PURGED = '2'
bnames = ['mogilefs-prod-repo', 'coprus', 'none', 'none', 'none', 'nane', 'plive']
ops = ['create-object', 'delete-object', 'delete', 'purge']
usedCnt = 0
delCnt = 0
purgeCnt = 0
objs = {}

sys.stdin.readline()
for l in sys.stdin:
  (bucketId, objkey, uuid, status, creationDate, timestamp) = parse_line(l)
  if not objs.has_key(objkey):
    objs[objkey] = []
  objs[objkey].append((bucketId, objkey, uuid, status, creationDate, timestamp))

  if status == STAT_USED:
    usedCnt += 1
    # print('#USED {o} {s} '.format(o=objkey, s=status))
  elif status == STAT_DEL:
    delCnt += 1
    print('#DELETED {o} {s} '.format(o=objkey, s=status))
  elif status == STAT_PURGED:
    purgeCnt += 1
    print('#PURGED {o} {s} '.format(o=objkey, s=status))

print('#Used: {c}'.format(c=str(usedCnt)))
print('#Delete: {c}'.format(c=str(delCnt)))
print('#Purged: {c}'.format(c=str(purgeCnt)))

updateStr = 'insert into audit (bucketName, keyValue, operation, uuid, timestamp)' + \
            ' VALUES ("{bn}", "{kv}", "{uid}", "{op}", "{ts}");   # SQL'
for k,v in objs.iteritems():
  if len(v) > 1:
    (bId, okey, uuid, st, cDate, tstamp) = v[len(v) - 1]
    print('#Multiples {0} {1} {2} {3} {4} {5} # MULTIPLES {6}'.format(bId, okey, uuid, st, cDate, tstamp, str(len(v))))

#  for l in v:
  (bId, okey, uuid, st, cDate, tstamp) = v[len(v)-1] 
  ndx = int(bId)-1
  if ndx >= len(bnames):
    print('#Bucket out of range: {0} {1} {2} {3} {4} {5}'.format(bId, okey, uuid, st, cDate, tstamp))
  else:
    bn = bnames[ndx]
    op = ops[int(st)]
    print(updateStr.format(bn=bn, kv=okey, uid=uuid, op=op, ts=cDate))
