#!/bin/bash
# args:
# $1 : marqo_image_name - name of the image you want to test

export LOCAL_OPENSEARCH_URL="https://localhost:9200"

docker rm -f marqo-os
docker run --name marqo-os -id -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" marqoai/marqo-os:0.0.3 &
# wait for marqo-os to start
until [[ $(curl -v --silent --insecure "$LOCAL_OPENSEARCH_URL/_aliases" 2>&1 | grep Unauthorized) ]]; do
    sleep 0.1;
done;


docker rm -f marqo
    docker run --name marqo --privileged -p 8882:8882 --add-host host.docker.internal:host-gateway \
        -e "OPENSEARCH_URL=$LOCAL_OPENSEARCH_URL" --memory=6g "$1" &
# wait for marqo to start
until [[ $(curl -v --silent --insecure http://localhost:8882 2>&1 | grep Marqo) ]]; do
    sleep 0.1;
done;

