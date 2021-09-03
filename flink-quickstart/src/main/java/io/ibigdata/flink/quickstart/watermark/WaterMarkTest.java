package io.ibigdata.flink.quickstart.watermark;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Map;

public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9099);

        SingleOutputStreamOperator<String> result = stream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                System.out.println("filter:" + s);
                return null != s && s.trim().length() > 0;
            }
        }).flatMap(new FlatMapFunction<String, Record>() {
            @Override
            public void flatMap(String s, Collector<Record> collector) throws Exception {
                Map<String, String> kv = JSON.parseObject(s, Map.class);
                String name = kv.get("name");
                String datetime = kv.get("datetime");

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long timestamp = sdf.parse(datetime).getTime();

                Record record = new Record();
                record.name = name;
                record.datetime = datetime;
                record.timestamp = timestamp;

                collector.collect(record);
            }
        }).assignTimestampsAndWatermarks(new MyWaterMark()).keyBy(line -> line.name)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new MyWindow()).name("watermark print");

        result.print();
        env.execute("watermark test");
    }
}
