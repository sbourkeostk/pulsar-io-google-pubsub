#!/usr/bin/env bash

set -e

SRC_DIR=$(git rev-parse --show-toplevel)
(cd "${SRC_DIR}" && mvn clean package -DskipTests)

(cd "${SRC_DIR}/.ci/integrations" && docker-compose up --remove-orphan --build --force-recreate -d)

CONTAINER_NAME="pulsar-io-google-pubsub-test"
PULSAR_ADMIN="docker exec -d ${CONTAINER_NAME} /pulsar/bin/pulsar-admin"

echo "Waiting for Pulsar service ..."
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
echo "Pulsar service available"

NAR_PATH="/test-pulsar-io-google-pubsub/pulsar-io-google-pubsub.nar"

SOURCE_NAME="test-pubsub-source"
DESTINATION_TOPIC="test-pubsub-source-topic"
SOURCE_CONFIG_FILE="/test-pulsar-io-google-pubsub/test-pulsar-io-google-pubsub-source.yaml"

echo "Run source connector"
eval "${PULSAR_ADMIN} sources localrun -a ${NAR_PATH} \
        --tenant public --namespace default --name ${SOURCE_NAME} \
        --source-config-file ${SOURCE_CONFIG_FILE} \
        --destination-topic-name ${DESTINATION_TOPIC}"

SOURCE_NAME="test-pubsub-source-avro"
DESTINATION_TOPIC="test-pubsub-source-avro-topic"
SOURCE_CONFIG_FILE="/test-pulsar-io-google-pubsub/test-pulsar-io-google-pubsub-source-avro.yaml"

echo "Run source connector with avro"
eval "${PULSAR_ADMIN} sources localrun -a ${NAR_PATH} \
        --tenant public --namespace default --name ${SOURCE_NAME} \
        --source-config-file ${SOURCE_CONFIG_FILE} \
        --destination-topic-name ${DESTINATION_TOPIC}"

SINK_NAME="test-pubsub-sink"
SINK_CONFIG_FILE="/test-pulsar-io-google-pubsub/test-pulsar-io-google-pubsub-sink.yaml"
INPUT_TOPIC="test-pubsub-sink-topic"

echo "Run sink connector"
eval "${PULSAR_ADMIN} sinks localrun -a ${NAR_PATH} \
        --tenant public --namespace default --name ${SINK_NAME} \
        --sink-config-file ${SINK_CONFIG_FILE} \
        -i ${INPUT_TOPIC}"

echo "Run sink connector with avro"
SINK_NAME="test-pubsub-sink-avro"
SINK_CONFIG_FILE="/test-pulsar-io-google-pubsub/test-pulsar-io-google-pubsub-sink-avro.yaml"
INPUT_TOPIC="test-pubsub-sink-avro-topic"

eval "${PULSAR_ADMIN} sinks localrun -a ${NAR_PATH} \
        --tenant public --namespace default --name ${SINK_NAME} \
        --sink-config-file ${SINK_CONFIG_FILE} \
        -i ${INPUT_TOPIC}"

echo "Waiting for sink and source ..."
sleep 60

echo "Run integration tests"
export PUBSUB_EMULATOR_HOST="localhost:8085"

(cd "$SRC_DIR" && mvn -Dtest="*IntegrationTest" test -DfailIfNoTests=false)

(cd "${SRC_DIR}/.ci/integrations" && docker-compose down --rmi local)
