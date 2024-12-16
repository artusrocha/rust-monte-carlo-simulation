#!/bin/sh

DUMP_FILE="$PWD/sample/montecarlo_db.sql.gz"

echo "### Creating Test Database Backup"
echo "### at $DUMP_FILE"
echo "###"

docker exec -it pg pg_dump -U montecarlo montecarlo | gzip > $DUMP_FILE 

if [ -f $DUMP_FILE ]; then
  echo "### Backup created" 
else
  echo "### Error !!!" 
  exit 1
fi