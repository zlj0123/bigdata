package myflink.sink.tidb;

import myflink.source.Student;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToTiDBMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("zookeeper.connect", "localhost:2181");
//        props.put("group.id", "metric-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "latest");
//
//        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
//                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
//                new SimpleStringSchema(),
//                props)).setParallelism(1)
//                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象

        SingleOutputStreamOperator<Student> student = env.fromElements(new Student(1, "name1", "pw1", 31),
                new Student(2, "name2", "pw2", 32),
                new Student(3, "name3", "pw3", 33),
                new Student(4, "name4", "pw4", 34),
                new Student(5, "name5", "pw5", 35));
        student.addSink(new SinkToTiDB()); //数据 sink 到 mysql

        env.execute("Flink add sink");
    }
}
