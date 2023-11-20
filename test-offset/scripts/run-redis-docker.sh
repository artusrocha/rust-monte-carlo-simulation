

docker run \
    -p 6379:6379 \
    --name rds \
    -v /tmp:/htmp  \
    -d redis
