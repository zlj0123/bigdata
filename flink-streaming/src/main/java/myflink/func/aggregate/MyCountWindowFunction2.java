package myflink.func.aggregate;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyCountWindowFunction2 implements WindowFunction<Long, String, String, TimeWindow> {
    @Override
    public void apply(String productId, TimeWindow window, Iterable<Long> input, Collector<String> out) throws Exception {
        /*商品访问统计输出*/
        /*out.collect("productId"productId,window.getEnd(),input.iterator().next()));*/
        out.collect("----------------窗口时间：" + window.getEnd());
        out.collect("商品ID: " + productId + "  浏览量: " + input.iterator().next());
    }
}
