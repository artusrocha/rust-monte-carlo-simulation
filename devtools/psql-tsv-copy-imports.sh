#!/bin/sh

alias psql='time docker exec -it pg psql -U montecarlo'

echo "### Importing /sample/product.tsv [$(wc -l sample/product.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_props FROM '/sample/product.tsv';"
echo ""

echo "### Importing /sample/product_batch.tsv [$(wc -l sample/product_batch.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_batch FROM '/sample/product_batch.tsv' WITH NULL as 'null';"
echo ""

echo "### Importing /sample/product_mov_hist.tsv to product_mov_hist [$(wc -l sample/product_mov_hist.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_mov_hist FROM '/sample/product_mov_hist.tsv';"
echo ""

echo "### Importing /sample/product_mov_hist.tsv to product_mov_hist_no_part [$(wc -l sample/product_mov_hist.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_mov_hist_no_part FROM '/sample/product_mov_hist.tsv';"
