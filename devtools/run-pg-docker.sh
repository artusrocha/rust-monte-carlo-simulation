

docker run \
    -p 5432:5432 \
    --name pg \
    -v $PWD/sample:/sample  \
    -e POSTGRES_PASSWORD=montecarlo \
    -e POSTGRES_USER=montecarlo \
    -d postgres
#export PG_CONN_STR='postgres://montecarlo:montecarlo@localhost:5432/montecarlo'
