#!/bin/bash

export PATH="$HOME/.rbenv/bin:$PATH" ; eval "$(rbenv init -)"


#
# monitors eads and removes deleted eads from the index. Then it tries re-indexing the deleted EAD, just in case it was re-added
#

source /home/arclight/.bash_profile

cd /home/arclight/scripts

file=/home/arclight/scripts/arclight_deletedEADsSorted.txt

echo "sorting file changes"
sort -u /home/arclight/scripts/arclight_deletedEADs.txt >> /home/arclight/scripts/arclight_deletedEADsSorted.txt


echo "killing fswatch"
xargs kill < /home/arclight/scripts/deletedStuffFswatch.pid

rm /home/arclight/scripts/arclight_deletedEADs.txt


echo "starting fswatch to watch for deleted eads"
fswatch -r -L /var/www/html/arclight/data/ead/  --event Removed >> /home/arclight/scripts/arclight_deletedEADs.txt &

#write pid to lockfile
echo $! > deletedStuffFswatch.pid
echo deletedStuffFswatch.pid

while read -r line
  do

##########################
#                        #
# delete from solr index #
#                        #  
##########################

#  ead_ssi=$(echo $line | sed -r s',^(\/.*\/(.*)\/(.*).xml)+,curl -X POST "localhost:8983/solr/arclight-core/update" -H "Content-Type: text/xml" --data-binary "<delete><query>(ead_ssi:\3)</query></delete>",')
   ead_ssi=$(echo $line | sed -r s',^(\/.*\/(.*)\/(.*).xml)+,curl -X POST "localhost:8984/solr/arclight/update" -H "Content-Type: text/xml" --data-binary "<delete><query>(ead_ssi:\3)</query><query>(id:\3_\*)</query></delete>",')

#  /${file2//./-}

  echo "$ead_ssi"
  echo "bleep"

  eval $ead_ssi


#ead_ssi=$(echo $line | sed -r 's,^.*/([^/]+)/([^/]+)\.xml$,curl -X POST "http://localhost:8983/solr/arclight-core2/update" -H "Content-Type: text/xml" --data-binary "<delete><query>(ead_ssi:\1_\2)</query></delete>",')
ead_ssi=$(echo $line | sed -r 's,^.*/([^/]+)/([^/]+)\.xml$,curl -X POST "http://localhost:8984/solr/arclight/update" -H "Content-Type: text/xml" --data-binary "<delete><query>(ead_ssi:\1_\2)</query><query>(ead_ssi:\1_)</query></delete>",')
ead_ssi=$(echo $ead_ssi | sed -r s'|findingaids|ctdbn|')

#  /${file2//./-}
  echo "new style, with repo prepended"
  echo "$ead_ssi"

  eval $ead_ssi



############################
#                          #
# Try re-indexing, in case #
# it was added back in     #
#                          #
############################

#  cd /home/arclight/arclight/

#  head -n 1  "$file"  >> /home/arclight/scripts/processedDeleted.txt   #copies first line to backup file

  runthis=$(echo $line | sed -r s',^(\/.*\/(.*)\/.*)+,FILE=\1 REPOSITORY_ID=\2 /home/arclight/.rbenv/versions/3.1.4/bin/bundle exec /home/arclight/.rbenv/versions/3.1.4/bin/rake arclight:index,')

  runthis=$(echo $runthis | sed -r s'|REPOSITORY_ID=findingaids|REPOSITORY_ID=ctdbn|')

  echo "$runthis"

  cd /home/arclight/wcsu-arclight1.1/

  eval $runthis

  echo "going back to scripts directory"
  cd /home/arclight/scripts
  sed -i -e "1d" $file   # deletes first line

  cd /home/arclight/wcsu-arclight1.1/

  line=

  runthis=

done < /home/arclight/scripts/arclight_deletedEADsSorted.txt

echo "all done :)"
