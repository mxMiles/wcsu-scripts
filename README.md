# wcsu-scripts

There are two scripts in this repository that, together, allow arclight to automatically detect changes to EADs and re-index them.

## monitorEADsForChanges.sh
The first script is monitorEADsForChanges.sh . It does what it says on the tin--it monitors the EADs directory for changes, then saves those changes to the file arclight_fileChanges.txt . This script is most useful if it is always running. To set it to always run, even after the machine reboots, you can add it to the arclight user's crontab.

open crontab in a terminal using the text editor nano (because nano is the best command-line text editor for quick and dirty editing.)
> EDITOR=nano crontab -e

Add the line (double-check that your script is in the same place, or modify as needed)
> @reboot    sleep  90 && source ~/.bash_profile && /home/arclight/scripts/monitorEADsForChanges.sh

This will run the script monitorEADsForChanges.sh 90 seconds after the server starts up.

## stripDatesAndEvents.sh
The second script is stripDatesAndEvents.sh . It reads in a line from arclight_fileChanges.txt, indexes the changed file, copies the line to processed.txt, and deletes the line from arclight_fileChanges. Then it moves on to the next line.

This script does not need to run constantly. Currently, it's set to run once an hour on the hour on the server. You can set this up using crontab as follows:
> EDITOR=nano crontab -e

Add the line
> 0 * * * *  ~/scripts/stripDatesAndEvents.sh

Visit crontab.guru for guidance on running the script at different times using crontab.

## /omeka2archivesspace/
Two scripts to copy digital objects from Omeka Classic to Archivesspace, via CSV.

### omekacsv.py
Exports items from Omeka S that have been modified in the last day, into a CSV file. Make sure the 'endpoint' variable points to the correct instance of Omeka.

### grabFromCSV.py
Imports digital objects from CSV into Archivesspace. Note: these digital objects aren't attached to any components, so you'll have to do that manually.

### Suggested crontab 
Runs once a day. Note that one script runs in python 2, the other in python 3.
> 0 12   * python /home/archivesspace/omekacsv.py; python3 /home/archivesspace/grabFromCSV.py 

## /archivesspaceUpdate/
Set of scripts to automate the updating of archviesspace. To use:
1. update config.sh to reflect latest versions
2. run install_new_version.sh
3. run start_new_release.sh

## omeka_s_harvester.py & omeka_s_grabFromCSV.py
Pair of scripts to export digital objects from Omeka-S, then shove them into ArchivesSpace. Warning: this is when I discovered emojis. I believe they make it easier to quickly scan log output :P
suggested crontab:
> 00 12 * * * python3 omeka_s_harvester.py; python2 /home/archivesspace/omeka_s_grabFromCSV.py
