#!/bin/bash

app='com.hncy58.kafka.monitor.KafkaTopicGroupOffsetsMonitor'
#app=${1}

echo 'start to stop app '${app}

#pids=`ps -ef|grep ${app}|grep -v grep|awk '{print $2}'`
pids=`cat app.pid`

echo 'start to kill process '${pids}

kill -15 ${pids}