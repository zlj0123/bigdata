#!/bin/bash


CLASS_NAME=flink.util.KafkaDataGenerator

nohup java -cp /root/flinkStreamSQL/exlib/flink-table-1.0.0.jar $CLASS_NAME &
