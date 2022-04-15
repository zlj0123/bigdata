package io.ibigdata.flink.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySqlBinlogSourceExample {
    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("zhanglijun") // monitor all tables under inventory database
                .tableList("products")
                .username("root")
                .password("Zlj840123")
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);
        env.addSource(sourceFunction).print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
