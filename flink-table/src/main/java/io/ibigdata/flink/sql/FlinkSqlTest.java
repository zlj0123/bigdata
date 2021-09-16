package io.ibigdata.flink.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSqlTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String sourceTable = "CREATE TABLE source_table (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '10',\n" +
                "  'fields.order_number.min' = '10',\n" +
                "  'fields.order_number.max' = '11'\n" +
                ")";

        String sinkTable = "CREATE TABLE sink_table (\n" +
                "    order_number BIGINT,\n" +
                "    price        DECIMAL(32,2)\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        String query = "insert into sink_table\n" +
                "select * from source_table\n" +
                "where order_number = 10";

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(sinkTable);
        tEnv.executeSql(query);
        env.execute("Sql Job");
    }
}
