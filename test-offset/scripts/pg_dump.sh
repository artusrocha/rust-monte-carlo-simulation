#!/bin/sh

DUMP_FILE="/tmp/pg2.sql.gz"

echo "### Creating Database Backup"
echo "### at $DUMP_FILE"
echo "###"

docker exec -it pg2 pg_dump -U pg2 pg2 | gzip > $DUMP_FILE 

if [ -f $DUMP_FILE ]; then
  echo "### Backup created" 
else
  echo "### Error !!!" 
  exit 1
fi