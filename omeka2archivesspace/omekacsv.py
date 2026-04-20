#! /usr/bin/python
# heavily modified from https://github.com/wcaleb/omekadd . 

from omekaclient import OmekaClient
import csv
import json
import math
import time
#import datetime
from datetime import datetime as dt
from datetime import timedelta

'''
You have to install pip httplib2.  sudo yum install python-pip . python -m pip install httplib2 . AND this needs to be in the same directory as omekaclient.py

'''
'''
Extract top-level metadata and element_texts from items returned by
Omeka 2.x API request, and then write to a CSV file. Intended for
requests to items, collections, element sets, elements, files, & tags.
'''

#yesterday = datetime.date.today() - datetime.timedelta(days=1)
yesterday = (dt.now() - timedelta(1)).strftime('%Y-%m-%d')
endpoint = 'http://grysbok.wcsu.edu/omeka/api/items'+'?modified_since='+str(yesterday)


apikey = None
resource = 'items'
print (yesterday)
print(endpoint)
#def request(query={"modified_since":str(yesterday)}):
def request(query={}):
    response, content = OmekaClient(endpoint, apikey).get(resource, None, query)
    if response.status != 200:
        print (response.status, response.reason)
        exit()
    else:
        print (response.status, response.reason)
        return response, content
def unicodify(v):
    if type(v) is bool or type(v) is int:
       return unicode(v)
    else:
       return v

def get_all_pages(pages):
    global data
#    page = pages
    page = 1
    while page <= pages:
        print ('Getting results page ' + str(page) + ' of ' + str(pages) + ' ...')
        response, content = request({'page': str(page), 'per_page': '50'})
        data.extend(json.loads(content))
        page += 1
        time.sleep(2)

# make initial API request; get max pages
response, content = request()
pages = int(math.ceil(float(response['omeka-total-results'])/50))
# pages= 1
#declare global variables; get all pages
fields = []
data = []
get_all_pages(pages)

for D in data:
    if 'tags' in D and D['tags']:
        tags = [ d['name'] for d in D['tags'] ]
        D['tags'] = ', '.join(tags)
    if 'element_texts' in D:
        for d in D['element_texts']:
            k = d['element']['name']
            v = d['text']
            D[k] = v
    for k, v in D.items():
        D[k] = unicodify(v)
        if D[k] and type(v) is dict:
            for key, value in v.items():
                D[k + '_' + key] = unicodify(D[k][key])
        if type(v) is list or type(v) is dict:
            del D[k] 
        if v == None:
            del D[k]
    for k in D.keys():
        if k not in fields: fields.append(k)

# write to CSV output file using DictWriter instance
# by default, fill empty cells with 'None'; un-quote None for empty cell
o = open(resource + '_output.csv', 'w')
c = csv.DictWriter(o, [f.encode('utf-8', 'replace') for f in sorted(fields)], restval='None', extrasaction='ignore') 
c.writeheader()
for D in data:
    c.writerow({k:v.encode('utf-8', 'replace') for k,v in D.items() if isinstance(v, unicode)})

o.close()
print ('File created: ' + resource + '_output.csv')
