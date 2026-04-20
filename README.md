# wcsu-scripts

There are two scripts in this repository that, together, allow arclight to automatically detect changes to EADs and re-index them.


## README.md
This lovely, handy file. Read it :)

## fixNoEADids.sh
This is a script we ran once against our CAO EADs.  When there is a null eadid, it causes arclight to punt when displaying a repoistories findingaids.  We built a routine into ArchivesSnake to build a temp EAD id to prevent it from occuring with cao_archivesspace.

## indexChecker.sh
A routine to check if files in the data directory are not live.  For maintenance.

## indexList.sh
Creatively enough, indexes all the items in the list "list.input".

## newRepo.sh
Puts together the directory structure needed to add a new repository to cao arclight. Syntax is
> ./newRepo.sh <repo id>

## processedIndexChecker.sh
Takes in the last 50 lines of processed.txt and checks to see if they're on the cao website

## removeDeletedFromIndex.sh
Run using a cron job. Watches the ead directories to see when items are deleted. When they're deleted from the file system they will be queued up to be deleted from SOLR/arclight.

## stripDatesAndEvents.sh
Poorly named workhorse of the scripts.

Monitors EAD files from changes and saves the filename of changed files to arclight_fileChanges.txt .

Then, it loops the items in arclight_fileChanges.txt, indexes the changed file, copies the line to processed.txt, and deletes the line from arclight_fileChanges. Then it moves on to the next line.

This script is most useful if it is always running. To set it to always run, even after the machine reboots, you can add it to the arclight user's crontab.

open crontab in a terminal using the text editor nano (because nano is the best command-line text editor for quick and dirty editing.)
> EDITOR=nano crontab -e

Add the line (double-check that your script is in the same place, or modify as needed)
> @reboot    sleep  90 && source ~/.bash_profile && /home/arclight/scripts/stripDatesAndEvents.sh

This will run the script stripDatesAndEvents.sh 90 seconds after the server starts up.

Visit crontab.guru for guidance on running the script at different times using crontab.

## watchBioFiles.sh
Watches /var/www/html/relatedObjects/BioFiles/ and puts changes into a file. Then Brian can go back and add these changes to the finding aid.
