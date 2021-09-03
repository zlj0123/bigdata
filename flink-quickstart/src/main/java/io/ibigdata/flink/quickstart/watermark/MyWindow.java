package io.ibigdata.flink.quickstart.watermark;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.TreeSet;

public class MyWindow implements WindowFunction<Record, String, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow window, Iterable<Record> input, Collector<String> out)
            throws Exception {
        TreeSet<Long> set = new TreeSet<>();
        //          元素个数
        int size = Iterables.size(input);
        Iterator<Record> eles = input.iterator();
        while (eles.hasNext()) {
            set.add(eles.next().timestamp);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //（code，窗口内元素个数，窗口内最早元素的时间，窗口内最晚元素的时间，窗口自身开始时间，窗口自身结束时间）
        String first = sdf.format(set.first());
        String last = sdf.format(set.last());
        String start = sdf.format(window.getStart());
        String end = sdf.format(window.getEnd());
        System.out.println("event.key:" + key + ",window中元素个数：" + size);
        // 调试使用
        out.collect("event.key:" + key + ",window中元素个数：" + size + ",window第一个元素时间戳：" + first + ",window最后一个元素时间戳："
                    + last + ",window开始时间戳：" + start + ",window结束时间戳：" + end + ",窗口内所有的时间戳：" + set.toString());
    }
}
