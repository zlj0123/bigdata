#!/bin/bash

export HADOOP_USER_NAME=hdfs
sh /root/flinkStreamSQL/bin/submit.sh -sql /root/flinkStreamSQL/job/kafka2kafka.sql -name flinkStreamSQL-test -localSqlPluginPath /root/flinkStreamSQL/sqlplugins/ -remoteSqlPluginPath /root/flinkStreamSQL/sqlplugins/ -mode yarnPer -flinkconf /opt/flink/conf/ -yarnconf /etc/hadoop/conf.cloudera.yarn -flinkJarPath /opt/flink/lib/ -pluginLoadMode shipfile -confProp \{\"time.characteristic\":\"EventTime\",\"logLevel\":\"debug\"}