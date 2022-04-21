#!/bin/bash

#######################  Variables ############################

# Source common variables
# source newRepoConfig.sh

NEW_REPO=$1 #reads first input from commandline

######################## Utilites ############################




# This is a script to run to create a new repository in CAO - run as root or sudo

# Create a data directory for ArcLight

echo "Creating repository directory for $NEW_REPO"

cd /var/www/html/arclight/data/ead/
    mkdir $NEW_REPO

echo "Creating StaticData inside $NEW_REPO"

cd /var/www/html/arclight/data/ead/$NEW_REPO
    mkdir staticData

echo "Creating the CSV contents of staticData"
# cp /home/arclight/scripts/{subjects.csv,collections.csv} /var/www/html/arclight/data/ead/$NEW_REPO/staticData
touch /var/www/html/arclight/data/ead/$NEW_REPO/staticData/subjects.csv
touch /var/www/html/arclight/data/ead/$NEW_REPO/staticData/collections.csv

echo "Changing ownership of all this stuff to our friend apache"
chown -R apache:apache /var/www/html/arclight/data/ead/$NEW_REPO
chmod -R g+rwx /var/www/html/arclight/data/ead/$NEW_REPO

echo "Open repository yml template and copy repo info into /home/arclight/arclight/config/repositories.yml and follow directions to create a webdav account and pw"

/usr/bin/firefox --new-window https://docs.google.com/spreadsheets/d/1ACTTfimD8Bh22mwTrQBUQDSste0Om_umjK8X27ZOaRI/edit?usp=sharing
 
echo "See ya" 
echo $?
