#!/bin/sh

alias psql='time docker exec -it pg psql -U montecarlo'

echo "### Importing /sample/product_props.tsv [$(wc -l sample/product_props.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_props FROM '/sample/product_props.tsv';"
echo ""

echo "### Importing /sample/product_batch.tsv [$(wc -l sample/product_batch.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_batch FROM '/sample/product_batch.tsv' WITH NULL as 'null';"
echo ""

echo "### Importing /sample/product_mov_hist.tsv to product_mov_hist [$(wc -l sample/product_mov_hist.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_mov_hist FROM '/sample/product_mov_hist.tsv';"
echo ""

echo "### Importing /sample/product_mov_hist.tsv to product_mov_hist_no_part [$(wc -l sample/product_mov_hist.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_mov_hist_no_part FROM '/sample/product_mov_hist.tsv';"
echo ""

echo "### Importing /sample/product_simulation_summary.tsv to product_simulation_summary [$(wc -l sample/product_simulation_summary.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_simulation_summary FROM '/sample/product_simulation_summary.tsv';"
echo ""

echo "### Importing /sample/product_simulation_summary_by_day.tsv to product_simulation_summary_by_day [$(wc -l sample/product_simulation_summary_by_day.tsv|cut -d' ' -f1) lines]"
psql -c "COPY product_simulation_summary_by_day FROM '/sample/product_simulation_summary_by_day.tsv';"
echo ""

echo "### Importing /sample/general_conf.tsv to general_conf [$(wc -l sample/general_conf.tsv|cut -d' ' -f1) lines]"
psql -c "COPY general_conf FROM '/sample/general_conf.tsv';"
echo ""