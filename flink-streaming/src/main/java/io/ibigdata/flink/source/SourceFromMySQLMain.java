package io.ibigdata.flink.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceFromMySQLMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new SourceFromMySQL()).print();
        env.execute("Flink add data sourc");
    }
}
