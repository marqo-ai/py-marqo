#!/bin/bash
# args:
# $1 : marqo_image_name - name of the image you want to test
docker rm -f marqo;
     docker run --name marqo --gpus all --privileged -p 8882:8882 --add-host host.docker.internal:host-gateway "$1" &
# wait for marqo to start
until [[ $(curl -v --silent --insecure http://localhost:8882 2>&1 | grep Marqo) ]]; do
    sleep 0.1;
done;
