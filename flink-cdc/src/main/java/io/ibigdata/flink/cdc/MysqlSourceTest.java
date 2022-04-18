package io.ibigdata.flink.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlSourceTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        //env.enableCheckpointing(3000);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql("CREATE TABLE mysql_binlog (\n" +
                "                id INT NOT NULL,\n" +
                "                name STRING,\n" +
                "                description STRING,\n" +
                "                weight DECIMAL(10,3),\n" +
                "                PRIMARY KEY(id) NOT ENFORCED\n" +
                "        ) WITH (\n" +
                "                'connector' = 'mysql-cdc',\n" +
                "                'hostname' = 'localhost',\n" +
                "                'port' = '3306',\n" +
                "                'scan.incremental.snapshot.chunk.size' = '40',\n" +
                "                'username' = 'root',\n" +
                "                'password' = 'Zlj840123',\n" +
                "                'database-name' = 'zhanglijun',\n" +
                "                'table-name' = 'products',\n" +
                "                'scan.startup.mode' = 'initial'\n" +
                "        )"
        );

/*        tEnv.executeSql(
                "CREATE TABLE mysql_binlog2 (\n" +
                        " id INT NOT NULL,\n" +
                        " name STRING,\n" +
                        " description STRING,\n" +
                        " weight DECIMAL(10,3),\n" +
                        " PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        " 'url' = 'jdbc:mysql://localhost:3306/zhanglijun',\n" +
                        " 'username' = 'root',\n" +
                        " 'password' = 'Zlj840123',\n" +
                        " 'table-name' = 'products_sink'\n" +
                        ")"
        );*/

        tEnv.executeSql("SELECT id, UPPER(name), description,weight FROM mysql_binlog").print();
        //tEnv.executeSql("insert into mysql_binlog2 select * from mysql_binlog").print();
        env.execute("Flink MySQL CDC testing");
    }
}
