#!/bin/bash

jar='kafka-0.0.1-SNAPSHOT.jar'
app='com.hncy58.kafka.monitor.KafkaTopicGroupOffsetsMonitor'
#app='com.hncy58.kafka.consumer.ConsumerToKuduApp'
#app='com.hncy58.kafka.consumer.ConsumerToHDFSApp'
#app=${1}

echo 'start to start app '${app}

nohup java -cp .:${jar} -Dtest=true ${app} 2>&1 >/dev/null & echo $! > app.pid