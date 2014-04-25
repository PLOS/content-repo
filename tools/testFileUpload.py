#!/usr/bin/env python2

import requests


baseUrl = 'http://localhost:8081/'

bucketProduction = 'plos-testupload-production'

r = requests.post(baseUrl + 'buckets/', data={'name' : bucketProduction})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'plos-testupload-staging'})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'plos-testupload-bad?name'})
print (r.text)
print (r.status_code)


files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'create' : 'new'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'create' : 'version'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'file':'file', 'create':'new'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'badContentType', 'bucketName' : bucketProduction, 'contentType' : 'text/plain', 'create':'new'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'downloadName', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'downloadName' : 'disTest.pdf', 'create':'new'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'downloadName', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'downloadName' : '10.1371/journal.pone.0002020.g003.PDF', 'create':'version'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)


# deletion test
files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'create' : 'version'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'create' : 'version'}
r = requests.post(baseUrl + 'objects/', files=files, data=values)
print (r.text)

values = {'key': 'devguide.pdf', 'version' : '1'}
r = requests.delete(baseUrl + 'objects/' + bucketProduction, params=values)
print (r.text)
