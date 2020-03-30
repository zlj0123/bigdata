package io.ibigdata.flink.quickstart.java.watermark;

import com.alibaba.fastjson.JSON;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws ParseException {
        String s = "{\"datetime\":\"2020-02-25 21:51:50\",\"name\":\"zhanglijun\"}";
        Map<String, String> kv = JSON.parseObject(s, Map.class);
        String name = kv.get("name");
        String datetime = kv.get("datetime");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long timestamp = sdf.parse(datetime).getTime();

        Record record = new Record();
        record.name = name;
        record.datetime = datetime;
        record.timestamp = timestamp;
    }
}
