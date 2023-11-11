#!/bin/sh

alias psql='docker exec -it pg psql -U montecarlo'

psql -c "DELETE FROM item;"

psql -c "DELETE FROM item_movement_historic;"

psql -c "DELETE FROM item_batch;"
