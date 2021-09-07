package io.ibigdata.flink.connector.kafka;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaSourceExample {
    public static void main(String[] args) throws Exception {
//        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
//                .hostname("10.20.30.113")
//                .port(33061)
//                .databaseList("test2") // monitor all tables under inventory database
//                .username("root")
//                .password("admin@123")
//                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
//                .build();


        KafkaSource<String> source = KafkaSource
                .<String>builder()
                .setBootstrapServers("10.20.30.113:9092")
                .setGroupId("testGroup2")
                .setTopics("zlj_test")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setUnbounded(OffsetsInitializer.latest())
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);
        env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source").setParallelism(4).print().setParallelism(4); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
