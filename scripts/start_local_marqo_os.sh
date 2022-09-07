# $1: the toxinidir, the path to the tox file.
# $2: the name of the marqo branch to test
# shellcheck disable=SC2155
export MARQO_API_TESTS_ROOT=$(pwd)
ls
. "$1/conf"
if [[ $(pwd | grep -v marqo-api-tests) ]]; then
  exit
fi
rm -rf "${MARQO_API_TESTS_ROOT}/temp"
mkdir "${MARQO_API_TESTS_ROOT}/temp"
cd "${MARQO_API_TESTS_ROOT}/temp" || exit
git clone https://github.com/marqo-ai/marqo.git
cd "${MARQO_API_TESTS_ROOT}/temp/marqo" || exit
git fetch
git switch "$2"

docker rm -f marqo-os &&
    docker run --name marqo-os -id -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" marqoai/marqo-os:0.0.2 &
# wait for marqo-os to start
until [[ $(curl -v --silent --insecure $OPENSEARCH_URL 2>&1 | grep Unauthorized) ]]; do
    sleep 0.1;
done;


docker rm -f marqo
DOCKER_BUILDKIT=1 docker build . -t marqo_docker_0 &&
    docker run --name marqo --privileged -p 8882:8882 --add-host host.docker.internal:host-gateway \
        -e "OPENSEARCH_URL=https://localhost:9200" marqo_docker_0 &
# wait for marqo to start
until [[ $(curl -v --silent --insecure http://localhost:8882 2>&1 | grep marqo) ]]; do
    sleep 0.1;
done;

