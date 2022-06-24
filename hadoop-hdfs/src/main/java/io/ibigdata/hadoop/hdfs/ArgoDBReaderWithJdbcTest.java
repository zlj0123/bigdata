package io.ibigdata.hadoop.hdfs;

import java.sql.*;

public class ArgoDBReaderWithJdbcTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test1();
    }

    public static void test1() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String jdbcURL = "jdbc:hive2://10.20.149.60:32244/yanch_test";
        Connection conn = DriverManager.getConnection(jdbcURL, "hive", "hive");

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("select * from yanch_test");
        ResultSetMetaData metaData = rs.getMetaData();

        int columnCount = metaData.getColumnCount();
        System.out.println("column count:" + columnCount);

        for (int i = 1; i <= columnCount; i++) {
            System.out.println("type[" + i + "] is:" + metaData.getColumnType(i));
        }
    }
}
