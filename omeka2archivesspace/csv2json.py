#import csv
#import json

#csvfile_path = open('/home/archivesspace/items_output.csv', 'r')
#jsonfile_path = open('/home/archivesspace/newDOOutput.json', 'w')

#with open('items_output.csv', mode='r') as csv_file:
#    csv_reader = csv.DictReader(csv_file)
#    line_count = 0
#    for row in csv_reader:
#        if line_count == 0:
#            line_count += 1

#field_name = ("Title","Identifier","url")
#reader = csv.DictReader( csvfile_path, field_name)
#out = json.dumps( {row['Title']},{row['Identifier']},{row['url']})
#jsonfile_path.write(out)



#import csv
#import json

#csvfile_path = open('/home/archivesspace/items_output.csv', 'r')
#jsonfile_path = open('/home/archivesspace/newDOOutput.json', 'w')

#field_name = ("Title","Identifier","url")
#reader = csv.DictReader( csvfile_path, field_name)
#out = json.dumps( [ row for row in reader ] )
#jsonfile_path.write(out)

import csv
import json

csvfile = open('/home/archivesspace/items_output.csv', 'r')
jsonfile = open('/home/archivesspace/newDOOutput.json', 'w')

#fieldnames = ("Title","Identifier","url")
#reader = csv.DictReader( csvfile, fieldnames)
#this bit to select the data from csv
full_dict = {csvfile}

keep = ['Title', 'Identifier', 'url']
partial_dict = {k: v for k, v in full_dict if k in keep}
#partial_dict = {k: v for k, v in full_dict.items() if k in keep}
#end of bit

for row in reader:
    json.dumps(partial_dict, jsonfile)
    jsonfile.write('\n')
