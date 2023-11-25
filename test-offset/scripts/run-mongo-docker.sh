

docker run \
    -p 27017:27017 \
    --name mongo \
    -e MONGO_INITDB_ROOT_USERNAME=test \
    -e MONGO_INITDB_ROOT_PASSWORD=e245gf456df9oisg \
    -v /tmp:/htmp  \
    -d mongo

