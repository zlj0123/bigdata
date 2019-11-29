package myflink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class FlinkTopN {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("zookeeper.connect", "localhost:2181");
        props.setProperty("group.id", "flink-kafka");
        URL fileUrl = FlinkRedisTopN.class.getClassLoader().getResource("itemId.txt");
        // 广播运营人员的配置文件，大家也可以实时读取mysql做广播
        DataStream<String> charge_battery_sn = env.readTextFile(fileUrl.getPath());
        MapStateDescriptor descriptor = new MapStateDescriptor("itemId_descriptor", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(String.class));
        BroadcastStream<String> charge_battery_sn_stream = charge_battery_sn.broadcast(descriptor);
        FlinkKafkaConsumer011 consumer = new FlinkKafkaConsumer011("flink3", new SimpleStringSchema(), props);
        //设置水位
        DataStream<String> order_stream = env.addSource(consumer).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String str) {
                return System.currentTimeMillis();
            }
        });
        // 两个流实时join，运营人员想要监控商品id 0001，0002，0003，
        // 所以商品id=0004的会被过滤掉
        DataStream<Tuple2<String, Long>> data = order_stream.connect(charge_battery_sn_stream).process(new BroadcastProcessFunction<String, String, Tuple2<String, Long>>() {

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                ReadOnlyBroadcastState<Object, Object> state = ctx.getBroadcastState(descriptor);
                try {
                    JSONObject json = JSON.parseObject(value);
                    if (state.contains(json.get("itemId").toString())) {
                        out.collect(new Tuple2(json.get("itemId").toString(), 1L));
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                BroadcastState state = ctx.getBroadcastState(descriptor);
                state.put(value, value);
            }
        });
        //打印两个流join后的效果
        data.print();
        data.keyBy(0).timeWindow(Time.seconds(5), Time.seconds(5))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Tuple2<String, Long> tuple2, Long acc) {
                        return acc + 1;
                    }

                    @Override
                    public Long getResult(Long acc) {
                        return acc;
                    }

                    @Override
                    public Long merge(Long acc1, Long acc2) {
                        return acc1 + acc2;
                    }
                }, new WindowFunction<Long, ItemIdCount, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow window, Iterable<Long> input, Collector<ItemIdCount> out) throws Exception {
                        String itemId = ((Tuple1<String>) key).f0;
                        Long count = input.iterator().next();
                        out.collect(ItemIdCount.of(itemId, window.getEnd(), count));
                    }
                }).keyBy("windowEnd").process(new TopNHotItems(3)).print();
        env.execute("kaishi");
    }
}

class TopNHotItems extends KeyedProcessFunction<Tuple, ItemIdCount, String> {
    private final int topSize;
    // 用于存储用户订单数量状态，待收齐同一个窗口的数据后，再触发 TopN 计算
    private ListState<ItemIdCount> itemIdCountListState;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 状态的注册
        ListStateDescriptor<ItemIdCount> itemsStateDesc = new ListStateDescriptor<>(
                "itemsState",
                ItemIdCount.class);
        itemIdCountListState = getRuntimeContext().getListState(itemsStateDesc);
    }

    @Override
    public void processElement(
            ItemIdCount input,
            Context context,
            Collector<String> collector) throws Exception {
        // 每条数据都保存到状态中
        itemIdCountListState.add(input);
        // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数
        context.timerService().registerProcessingTimeTimer(input.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取所有商品的订单信息
        List<ItemIdCount> allUserOrder = new ArrayList<>();
        for (ItemIdCount item : itemIdCountListState.get()) {
            allUserOrder.add(item);
        }
        // 提前清除状态中的数据，释放空间
        itemIdCountListState.clear();
        // 按照订单量从大到小排序
        allUserOrder.sort(new Comparator<ItemIdCount>() {
            @Override
            public int compare(ItemIdCount o1, ItemIdCount o2) {
                return (int) (o2.orderCount - o1.orderCount);
            }
        });
        for (int i = 0; i < topSize; i++) {
            if (i <= allUserOrder.size() - 1) {
                ItemIdCount userIdCount = allUserOrder.get(i);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("itemId", userIdCount.itemId);
                jsonObject.put("orderQt", userIdCount.orderCount);
                out.collect(jsonObject.toString());
            }
        }
    }
}