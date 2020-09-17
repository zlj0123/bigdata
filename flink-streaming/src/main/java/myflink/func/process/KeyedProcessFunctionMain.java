package myflink.func.process;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedProcessFunctionMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        //evn.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*指定数据源 从socket的9000端口接收数据，先进行了不合法数据的过滤*/
        DataStream<String> sourceDS = evn.socketTextStream("10.20.30.112", 9099)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        if (null == line || "".equals(line)) {
                            return false;
                        }
                        String[] lines = line.split(",");
                        if (lines.length != 2) {
                            return false;
                        }
                        return true;
                    }
                });

        // the source data stream
        DataStream<Tuple2<String, String>> stream = sourceDS.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] lines = line.split(",");
                return new Tuple2<String, String>(lines[0], lines[1]);
            }
        });

        // apply the process function onto a keyed stream
        DataStreamSink<Tuple2<String, Long>> result = stream
                .keyBy(value -> value.f0).process(new CountWithTimeoutFunction()).print();

        evn.execute("KeyedProcessFunction Testing");
    }
}