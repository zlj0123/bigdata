package io.ibigdata.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MysqlSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(30000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE mysql_binlog (\n" +
                        " id INT NOT NULL,\n" +
                        " age INT,\n" +
                        " address STRING,\n" +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'mysql-cdc',\n" +
                        " 'hostname' = '10.20.30.113',\n" +
                        " 'port' = '33061',\n" +
                        " 'scan.incremental.snapshot.chunk.size' = '40',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'admin@123',\n" +
                        " 'database-name' = 'test2',\n" +
                        " 'table-name' = 'flink_cdc_test2'\n" +
                        ")"
        );

//        tEnv.executeSql(
//                "CREATE TABLE mysql_binlog2 (\n" +
//                        " id INT NOT NULL,\n" +
//                        " age INT,\n" +
//                        " address STRING,\n" +
//                        " PRIMARY KEY(id) NOT ENFORCED\n" +
//                        ") WITH (\n" +
//                        " 'connector' = 'jdbc',\n" +
//                        " 'url' = 'jdbc:mysql://localhost:3306/zhanglijun',\n" +
//                        " 'username' = 'root',\n" +
//                        " 'password' = 'Zlj840123',\n" +
//                        " 'table-name' = 'flink_cdc_test_bk'\n" +
//                        ")"
//        );

        Table t = tEnv.sqlQuery("SELECT id, age,UPPER(address) FROM mysql_binlog");
        tEnv.toRetractStream(t, Row.class).print();
//        tEnv.executeSql("insert into mysql_binlog2 select * from mysql_binlog").print();
        env.execute("Flink MySQL CDC testing");
    }
}
