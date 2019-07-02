package org.apache.flink.streaming.scala.examples.kafka;

import java.util.Map;

/**
 * @author ZhangLijun
 * @Title: Metric
 * @ProjectName bigdata-flink
 * @Description: TODO
 * @contact: ljzhang@icarevision.cn
 * @date 2019-06-2421:14
 */
public class Metric {
    public String name;
    public long timestamp;
    public Map<String, Object> fields;
    public Map<String, String> tags;

    public Metric() {
    }

    public Metric(String name, long timestamp, Map<String, Object> fields, Map<String, String> tags) {
        this.name = name;
        this.timestamp = timestamp;
        this.fields = fields;
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "name='" + name + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", fields=" + fields +
                ", tags=" + tags +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setFields(Map<String, Object> fields) {
        this.fields = fields;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
