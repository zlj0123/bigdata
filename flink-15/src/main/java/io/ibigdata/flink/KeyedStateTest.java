package io.ibigdata.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute("Keyed State Test");
// the printed output will be (1,4) and (1,5)
    }
}
