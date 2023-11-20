

docker run \
    -p 5433:5432 \
    --name pg2 \
    -v /tmp:/htmp  \
    -e POSTGRES_PASSWORD=pg2 \
    -e POSTGRES_USER=pg2 \
    -d postgres
#export PG_CONN_STR='postgres://montecarlo:montecarlo@localhost:5432/montecarlo'
