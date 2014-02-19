#!/usr/bin/env python2

import requests, json
import re
from lxml import etree


baseUrl = 'http://localhost:8080/'

bucketProduction = 'plos-testupload-production'

r = requests.post(baseUrl + 'buckets/', data={'name' : bucketProduction, 'id':0})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'plos-testupload-staging'})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'plos-testupload-bad?name', 'id':2})
print (r.text)
print (r.status_code)


files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'newAsset' : 'true'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'newAsset' : False}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'file':'file', 'newAsset':True}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'badContentType', 'bucketName' : bucketProduction, 'contentType' : 'text/plain', 'newAsset':True}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'downloadName', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'downloadName' : 'disTest.pdf', 'newAsset':True}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)


# deletion test
files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'newAsset' : False}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : bucketProduction, 'contentType' : 'application/pdf', 'newAsset' : False}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

values = {'key': 'devguide.pdf', 'version' : '1'}
r = requests.delete(baseUrl + 'assets/' + bucketProduction, params=values)
print (r.text)
