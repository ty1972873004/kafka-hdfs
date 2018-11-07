#!/bin/bash

app='com.hncy58.kafka.consumer.ConsumerToKuduApp'
app=${1}

echo 'start to stop app '${app}

pids=`ps -ef|grep ${app}|grep -v grep|awk '{print $2}'`

kill -15 ${pids}