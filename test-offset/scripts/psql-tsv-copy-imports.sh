#!/bin/sh

alias psql='time docker exec -it pg psql -U montecarlo'

echo "### Importing /sample/data/item.tsv [$(wc -l sample/data/item.tsv|cut -d' ' -f1) lines]"
psql -c "COPY item FROM '/sample/data/item.tsv';"
echo ""

echo "### Importing /sample/data/item_batch.tsv [$(wc -l sample/data/item_batch.tsv|cut -d' ' -f1) lines]"
psql -c "COPY item_batch FROM '/sample/data/item_batch.tsv' WITH NULL as 'null';"
echo ""

echo "### Importing /sample/data/item_mov_hist.tsv to item_mov_hist [$(wc -l sample/data/item_mov_hist.tsv|cut -d' ' -f1) lines]"
psql -c "COPY item_mov_hist FROM '/sample/data/item_mov_hist.tsv';"
echo ""

echo "### Importing /sample/data/item_mov_hist.tsv to item_mov_hist_no_part [$(wc -l sample/data/item_mov_hist.tsv|cut -d' ' -f1) lines]"
psql -c "COPY item_mov_hist_no_part FROM '/sample/data/item_mov_hist.tsv';"
