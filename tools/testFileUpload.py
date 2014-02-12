#!/usr/bin/env python2

import requests, json
import re
from lxml import etree


baseUrl = 'http://localhost:8080/'

r = requests.post(baseUrl + 'buckets/', data={'name' : 'production', 'id':0})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'staging'})
print (r.text)
# r = requests.post(baseUrl + 'buckets/', data={'name' : 'bad?name', 'id':2})
# print (r.text)
# print (r.status_code)


# files = {'file': open('/home/jfinger/mounts.log', 'rb')}
# values = {'key': 'testfile.log', 'bucketName' : 'production', 'contentType' : 'text/plain', 'newAsset' : 'true'}
# r = requests.post(baseUrl + 'assets/', files=files, data=values)
# print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'newAsset' : 'true'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'newAsset' : False}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'file':'file', 'newAsset':True}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'badContentType', 'bucketName' : 'production', 'contentType' : 'text/plain', 'newAsset':True}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'downloadName', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'downloadName' : 'disTest.pdf', 'newAsset':True}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)


# deletion test
files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'newAsset' : False}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'newAsset' : False}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

values = {'key': 'devguide.pdf', 'version' : '1'}
r = requests.delete(baseUrl + 'assets/production', params=values)
print (r.text)
