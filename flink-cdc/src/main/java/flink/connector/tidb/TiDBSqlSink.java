package flink.connector.tidb;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TiDBSqlSink {
    public static void main(String[] args) {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.enableCheckpointing(10000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv);
        DataStream<Student> dataStream = bsEnv.addSource(new StudentSource());
        String sql = "CREATE TABLE MyStudentTable (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  password STRING,\n" +
                "  age INT\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://10.20.145.29:4000/bigdata',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456',\n" +
                "   'table-name' = 'student'\n" +
                ")";
        tEnv.executeSql(sql);
        tEnv.createTemporaryView("students", dataStream, $("id"), $("name"), $("password"), $("age"));
        String insertSql = "insert into MyStudentTable SELECT id, name, password,age FROM students";

        tEnv.executeSql(insertSql);
    }
}
