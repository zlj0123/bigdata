package io.ibigdata.flink.connector.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql(
                "CREATE TABLE kafka_table (\n" +
                        " id INT NOT NULL,\n" +
                        " age INT,\n" +
                        " address STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'kafka',\n" +
                        " 'topic' = 'zlj_test',\n" +
                        " 'properties.bootstrap.servers' = '10.20.30.113:9092',\n" +
                        " 'properties.group.id' = 'testGroup1',\n" +
                        " 'scan.startup.mode' = 'earliest-offset',\n" +
                        " 'format' = 'csv'\n" +
                        ")"
        );

        Table t = tEnv.sqlQuery("SELECT id, age,UPPER(address) FROM kafka_table");
        tEnv.toRetractStream(t, Row.class).print();
        env.execute();
    }
}
