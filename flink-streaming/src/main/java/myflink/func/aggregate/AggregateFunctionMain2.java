package myflink.func.aggregate;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AggregateFunctionMain2 {

    public static int windowSize = 6000;/*滑动窗口大小*/
    public static int windowSlider = 3000;/*滑动窗口滑动间隔*/

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /*DataStream<String> sourceData = senv.socketTextStream("localhost",9000);*/
        //从文件读取数据，也可以从socket读取数据
        DataStream<String> sourceData = senv.readTextFile("./inout/product_view_data.txt");
        DataStream<ProductViewData> productViewData = sourceData.map(new MapFunction<String, ProductViewData>() {
            @Override
            public ProductViewData map(String value) throws Exception {
                String[] record = value.split(",");
                return new ProductViewData(record[0], record[1], Long.valueOf(record[2]), Long.valueOf(record[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ProductViewData>() {
            @Override
            public long extractAscendingTimestamp(ProductViewData element) {
                return element.timestamp;
            }
        });
        /*过滤操作类型为1  点击查看的操作*/
        DataStream<String> productViewCount = productViewData.filter(new FilterFunction<ProductViewData>() {
            @Override
            public boolean filter(ProductViewData value) throws Exception {
                if (value.operationType == 1) {
                    return true;
                }
                return false;
            }
        }).keyBy(new KeySelector<ProductViewData, String>() {
            @Override
            public String getKey(ProductViewData value) throws Exception {
                return value.productId;
            }
            //时间窗口 6秒  滑动间隔3秒
        }).timeWindow(Time.milliseconds(windowSize), Time.milliseconds(windowSlider))
                /*这里按照窗口进行聚合*/
                .aggregate(new MyCountAggregate(), new MyCountWindowFunction2());
        //聚合结果输出
        productViewCount.print();

        senv.execute("AggregateFunctionMain2");
    }
}
