#!/bin/sh

alias psql='docker exec -it pg psql -U montecarlo'

psql -c "COPY item FROM '/sample/data/item.tsv';"

psql -c "COPY item_movement_historic FROM '/sample/data/item_mov_hist.tsv';"

psql -c "COPY item_batch FROM '/sample/data/item_batch.tsv' WITH NULL as 'null';"
