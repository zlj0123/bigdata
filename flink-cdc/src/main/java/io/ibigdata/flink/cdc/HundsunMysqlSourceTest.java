package io.ibigdata.flink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HundsunMysqlSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog (\n" +
                        " id INT NOT NULL,\n" +
                        " age INT,\n" +
                        " address STRING,\n" +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '10.20.30.113',\n" +
                        " 'port' = '33061',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = '123456',\n" +
                        " 'database-name' = 'zhanglijun',\n" +
                        " 'table-name' = 'flink_cdc_source',\n" +
                        " 'server-id' = '123456789',\n" +
                        " 'scan.startup.mode' = 'initial'\n" +
                        ")"
        );

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog2 (\n" +
                        " id INT NOT NULL,\n" +
                        " age INT,\n" +
                        " address STRING,\n" +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = 'jdbc:mysql://10.20.30.113:33061/zhanglijun',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = '123456',\n" +
                        " 'sink.buffer-flush.max-rows' = '1',\n" +
                        " 'sink.buffer-flush.interval' = '1s',\n" +
                        " 'table-name' = 'flink_cdc_sink'\n" +
                        ")"
        );

        //tEnv.executeSql("SELECT id, age,UPPER(address) FROM mysql_binlog").print();

        tEnv.executeSql("insert into mysql_binlog2 select * from mysql_binlog").print();
        env.execute();
    }
}

