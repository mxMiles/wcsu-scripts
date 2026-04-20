#!/bin/bash

# This script is run as archivesspace user after the new version has been
# installed.  It shuts down the previous relase, runs the database setup script
# for the new release and then starts the application. 

# common variables
source config.sh

# stop the current running application
/home/archivesspace/$PREVIOUS_RELEASE/archivesspace/archivesspace.sh stop

# update the database for this release
# added a test for existence in release 1.5.3
if [ -e "/home/archivesspace/$NEW_RELEASE/archivesspace/scripts/setup-database.sh" ]
then 
  /home/archivesspace/$NEW_RELEASE/archivesspace/scripts/setup-database.sh
fi

# start the updated app
/home/archivesspace/$NEW_RELEASE/archivesspace/archivesspace.sh start

