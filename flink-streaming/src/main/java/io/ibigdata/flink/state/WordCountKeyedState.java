package io.ibigdata.flink.state;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WordCountKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> wordDS = env.addSource(new WordDataSource());

        SingleOutputStreamOperator<Tuple2<String, Integer>> totalCntDS =
                wordDS.map(w -> Tuple2.of(w, 1), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                }))
                        .keyBy(t -> t.f0, TypeInformation.of(new TypeHint<String>() {
                        }))
                        .map(new WordCountReducingFunction());

        totalCntDS.print();

        env.execute("Word Count By Reducing State!");
    }
}
