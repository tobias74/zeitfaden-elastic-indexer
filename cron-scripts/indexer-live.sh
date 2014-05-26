#!/bin/bash

exec 9>/var/tmp/indexer-live-lock-file
if ! flock -n 9  ; then
   echo "another instance is running";
   exit 1
fi

java -jar ~/zeitfaden-elastic-indexer/target/zeitfaden-elastic-indexer-0.1.0-SNAPSHOT-standalone.jar -vv -t live -s 500 -l 5


# this now runs under the lock until 9 is closed (it will be closed automatically when the script ends)




