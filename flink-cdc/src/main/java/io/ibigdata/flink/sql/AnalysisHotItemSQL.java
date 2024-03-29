package io.ibigdata.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class AnalysisHotItemSQL {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        fsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        DataStream<String> inputStream = fsEnv.readTextFile(".\\inout\\UserBehavior.csv");

        DataStream<UserBehavior> dataStream = inputStream.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String s) throws Exception {
                String[] dataArray = s.split(",");
                return new UserBehavior(Long.parseLong(dataArray[0]), Long.parseLong(dataArray[1]), Integer.parseInt(dataArray[2]), dataArray[3], Long.parseLong(dataArray[4]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(UserBehavior element) {
                return element.timestamp * 1000L;
            }
        });

        // 创建临时表
        tableEnv.createTemporaryView("UserBehavior", dataStream);

        String sql = ("select * from (select *,row_number() over(partition by windowEnd order by cnt desc) as row_num from(select itemId, count(itemId) as cnt, hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd from " +
                "UserBehavior where behavior = 'pv' group by itemId, hop(ts, interval '5' minute, interval '1' hour))) where row_num <= 5").trim();

        Table topNResultTable = tableEnv.sqlQuery(sql);
        String[] names = new String[] {"itemId" ,"cnt" ,"windowEnd" , "row_num"};
        TypeInformation[] types = new TypeInformation[] {Types.JAVA_BIG_INT(),Types.JAVA_BIG_INT(),Types.SQL_TIMESTAMP(), Types.JAVA_BIG_INT()};

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(topNResultTable, Types.ROW(names, types));
        tuple2DataStream.print();

        fsEnv.execute("Top PV");
    }
}
