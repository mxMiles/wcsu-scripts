#!/usr/bin/env python3
# takes a CSV file (omeka_s_items_output.csv) of Omeka items created in the last 24 hours
# and pushes those digital objects into ArchivesSpace

import os
import sys
import dacs
import time
import csv
import shutil
import requests
import json
from asnake.client import ASnakeClient
import asnake.logging as logging

print ("\tConnecting to ArchivesSpace")
client = ASnakeClient(baseurl="http://localhost:####",
                      username="#####",
                      password="#####")
client.authorize()

logging.setup_logging(stream=sys.stdout, level='INFO')

with open('omeka_s_items_output.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1

        # skip items from wrong omeka-s site
        site=str(row['o:site'])
        if site=='http://ruth.wcsu.edu/omeka-s/api/sites/1':
                title=str(row['dcterms:title'])
                identifier=str(row['dcterms:identifier'])
                urlRaw=str(row['public_url'])
                url=urlRaw.replace("http://ruth.wcsu.edu/omeka-s/s/digital/item/","https://archives.library.wcsu.edu/omeka-s/s/digital/item/")

                data = { "jsonmodel_type":"digital_object",
                       "file_versions": [{
                       "jsonmodel_type":"file_version",
                       "file_uri":url,
                       "is_representative":False,
                       "caption":title+" ["+url+"]",
                       "use_statement":"Image-Service",
                       "publish":True}],
                       "digital_object_id":identifier,
                       "title":title}

                r = client.post('repositories/3/digital_objects', json=data)

                print(json.dumps(data))
        else: 
                print("💥 skipping because item is from the wrong site (this is fine)")
      
