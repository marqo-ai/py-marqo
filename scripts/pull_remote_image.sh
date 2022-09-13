# args:
# $1 : marqo_image_name - name of the image you want to test
if [[ $1 != "marqo_docker_01" ]]; then
  docker pull $1
fi