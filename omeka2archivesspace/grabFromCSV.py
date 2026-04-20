#!/usr/bin/env python3
import os
import sys
import dacs
import time
import csv
import shutil

import csv
import requests
import json
from asnake.client import ASnakeClient
import asnake.logging as logging


print ("\tConnecting to ArchivesSpace")

client = ASnakeClient(baseurl="http://localhost:8092",
                      username="xxxx",
                      password="xxxxxxxx")
client.authorize()

logging.setup_logging(stream=sys.stdout, level='INFO')

with open('items_output.csv', mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            line_count += 1

  
        title=str(row['Title'])
        identifier=str(row['Identifier'])
        urlRaw=str(row['url']
# Have to clean the URL        
        url=urlRaw.replace("/api/items","/items/show")
#        print(title+identifier+url)

#        file_version = {
#                "jsonmodel_type":"digital_object",
#                "file_uri":url,
#                "is_representative":False,
#                "caption":title+" ["+url+"]",
#                "use_statement":"Image-Service",
#                "publish":True}

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

        r = client.post('repositories/3/digital_objects', json.dumps(data))

        print(json.dumps(data))

