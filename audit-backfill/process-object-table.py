#!/usr/bin/env python
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

