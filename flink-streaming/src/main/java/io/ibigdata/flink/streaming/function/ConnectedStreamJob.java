package io.ibigdata.flink.streaming.function;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ConnectedStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(x -> x);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        control.connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }
}
