#!/bin/bash

app='com.hncy58.kafka.consumer.ConsumerToKuduApp'
app=${1}

echo 'start to start app '${app}

java -cp .:kafka-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${app}