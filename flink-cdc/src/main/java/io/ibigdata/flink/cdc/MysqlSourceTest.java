package io.ibigdata.flink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MysqlSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
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
                        " 'password' = 'admin@123',\n" +
                        " 'database-name' = 'test2',\n" +
                        " 'table-name' = 'test2_dest'\n" +
                        ");"
        );

        Table t = tEnv.sqlQuery("ELECT id, age,UPPER(address) FROM mysql_binlog");

        tEnv.toAppendStream(t, Row.class).print();

        env.execute();
    }
}
