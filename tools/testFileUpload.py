
import requests
import re
from lxml import etree


baseUrl = 'http://localhost:8080/'

r = requests.post(baseUrl + 'buckets/', data={'name' : 'production', 'id':0})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'staging'})
print (r.text)
r = requests.post(baseUrl + 'buckets/', data={'name' : 'bad?name', 'id':2})
print (r.text)


files = {'file': open('/home/jfinger/mounts.log', 'rb')}
values = {'key': 'testfile.log', 'bucketName' : 'production', 'contentType' : 'text/plain'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : 'production', 'contentType' : 'application/pdf'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/journal.pone.0085647.pdf', 'rb')}
values = {'key': 'devguide.pdf', 'bucketName' : 'production', 'contentType' : 'application/pdf'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide', 'bucketName' : 'production', 'contentType' : 'application/pdf'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'devguide', 'bucketName' : 'production', 'contentType' : 'application/pdf'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'badContentType', 'bucketName' : 'production', 'contentType' : 'text/plain'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

files = {'file': open('/home/jfinger/looseFiles/DH3.8_DeveloperGuide.pdf', 'rb')}
values = {'key': 'contentDisposition', 'bucketName' : 'production', 'contentType' : 'application/pdf', 'contentDisposition' : 'disTest.pdf'}
r = requests.post(baseUrl + 'assets/', files=files, data=values)
print (r.text)

