package io.ibigdata.flink.quickstart.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;

public class MyWaterMark implements AssignerWithPeriodicWatermarks<Record> {
    //定义最大延迟 10s
    private final long maxOutOfOrderness = 10000L;
    //将时间戳信息格式化,调试学习
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private long currentMaxTimestamp;
    private Watermark watermark;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        watermark = new Watermark(this.currentMaxTimestamp - this.maxOutOfOrderness);
        return watermark;
    }

    @Override
    public long extractTimestamp(Record element, long previousElementTimestamp) {
        //      获取event中的时间戳
        long timestamp = element.timestamp;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        //          将所有的时间信息打印
        System.out.println("currentThreadId:" + Thread.currentThread().getId()
                           + ",key:" + element.name + ",eventTime:[" + element.datetime + "],currentMaxTimestamp:["
                           + sdf.format(currentMaxTimestamp) + "],watermark:["
                           + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
        //返回event中的时间戳
        return timestamp;
    }
}
