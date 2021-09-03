package io.ibigdata.flink.connector.file;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamingWriteFile {
    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.enableCheckpointing(10000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);
        DataStream<UserInfo> dataStream = bsEnv.addSource(new MySource());
        String sql = "CREATE TABLE fs_table (\n" +
                "  user_id STRING,\n" +
                "  order_amount DOUBLE,\n" +
                "  dt STRING," +
                "  h string," +
                "  m string  \n" +
                ") PARTITIONED BY (dt,h,m) WITH (\n" +
                "  'connector'='filesystem',\n" +
                "  'path'='file:///E:\\GitHub\\',\n" +
                "  'format'='orc'\n" +
                ")";
        tEnv.executeSql(sql);
        tEnv.createTemporaryView("users", dataStream);
        String insertSql = "insert into  fs_table SELECT userId, amount, " +
                " DATE_FORMAT(ts, 'yyyy-MM-dd'), DATE_FORMAT(ts, 'HH'), DATE_FORMAT(ts, 'mm') FROM users";

        tEnv.executeSql(insertSql);
    }
}
