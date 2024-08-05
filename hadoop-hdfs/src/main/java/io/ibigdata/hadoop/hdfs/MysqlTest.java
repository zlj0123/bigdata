package io.ibigdata.hadoop.hdfs;

import java.sql.*;

public class MysqlTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test1();
    }

    public static void test1() throws SQLException, ClassNotFoundException{
        String driver = "com.mysql.cj.jdbc.Driver";
        String jdbcURL = "jdbc:mysql://10.20.30.113:33061/zhanglijun";
        String username = "root";
        String password = "123456";

        Class.forName(driver);

        String metaDataSql = "SELECT @row_number := @row_number + 1 AS id from (SELECT @row_number := 0) t where 1=2";
        try (Connection connection = DriverManager.getConnection(jdbcURL,username,password);
             Statement stmt = connection.createStatement()) {
            ResultSet metaRs = stmt.executeQuery(metaDataSql);
            ResultSetMetaData rsMetaData = metaRs.getMetaData();
            System.out.println("---------meta info----------");
            for (int i=0,len = rsMetaData.getColumnCount();i<len;i++) {
                System.out.println("column_name:" + rsMetaData.getColumnName(i+1) + "   " + "column_type:" + rsMetaData.getColumnType(i+1));
            }
        }
    }
}
