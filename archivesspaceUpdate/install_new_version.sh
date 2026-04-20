#!/bin/bash

#######################  Variables ############################

# Source common variables
source config.sh
BACKUP_DATE=$(date +"%Y-%m-%d.zip")

######################## Utilites ############################

function run_backup() {
    # this function is run from the arhivesspace directory of the previouse
    # version
    if ! scripts/backup.sh --mysqldump --output ../backups/$BACKUP_DATE
    then
        echo "Backup script failed. You'd better check it out."
        exit 1
    fi
}

###################### Main #################################


# create a backup of existing app
# you have to run backup script from withn archivesspace directory of the preiouse release
# because the script appears to be  using relative paths.

# we start in achivesspace home directory

echo "Creating a backup of current version"

cd /home/archivesspace/$PREVIOUS_RELEASE/archivesspace
if [ ! -d "../backups" ]
then
    mkdir ../backups
    run_backup
else
    # check if there is a backup with today's date
    # if not run the backup script
    if [[ ! -f ../backups/$BACKUP_DATE ]]
    then
        run_backup
    fi
fi

# change back to archivesspace home dir and get create the new release
# make a directory for the new release
# change to that directory and download the new release.
# the url comes from te config.sh

echo "Downloading and creating the new release."
cd /home/archivesspace
mkdir  $NEW_RELEASE
cd $NEW_RELEASE

# download the release and unzip it
# the file is stored at Amazon so the name is too long for wget
# so you need to specify the output file
if ! wget -O "$NEW_RELEASE.zip" "$RELEASE_URL"
then
    echo "Downloading the new release failed"
    exit 1
else
    unzip *.zip
fi

echo " "
echo "Copying data files over to new version"
echo "This can take a while!"

# save the release config file, things might have changed
cp archivesspace/config/config.rb archivesspace/config/config.rb.$NEW_RELEASE

# copy data to new release from previouse release
cp -a /home/archivesspace/$PREVIOUS_RELEASE/archivesspace/config/*  archivesspace/config/
cp -a /home/archivesspace/$PREVIOUS_RELEASE/archivesspace/data/*   archivesspace/data/
cp -a /home/archivesspace/$PREVIOUS_RELEASE/archivesspace/lib/mysql-connector* archivesspace/lib/
cp -a /home/archivesspace/$PREVIOUS_RELEASE/archivesspace/plugins/* archivesspace/plugins/
cp -a /home/archivesspace/$PREVIOUS_RELEASE/archivesspace/locales/* archivesspace/locales/ #added 2021/03/11

# make all files owned by archives space user
chown -R archivesspace:archivesspace /home/archivesspace

echo "You can run the startup script now"
