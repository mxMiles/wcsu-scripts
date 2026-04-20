These scripts were stacked in order to automate the creation of digital "items" in our instance of Omeka and the creation of digital instances in ArchivesSpace.

First, we modified wcaleb's (https://github.com/wcaleb/omekadd) script to grab csv from Omeka's API - omekacsv.py (it's dependencies are omekaclient.py, omekaclient.pyc).
It creates a items_output.csv file based on an endpoint of items created since yesterday.

grabFromCSV.py then is run to "grab" elements from the csv file and adds them to a JSON object that is pushed into ArchivesSpace via its API.

The omekacsv.py (python) and grabFromCSV.py (python3) are run as cron jobs.
