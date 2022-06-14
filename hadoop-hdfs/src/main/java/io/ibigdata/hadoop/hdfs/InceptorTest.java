package io.ibigdata.hadoop.hdfs;

import java.sql.*;

public class InceptorTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test3();
    }

    public static void test1() throws SQLException, ClassNotFoundException{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String jdbcURL = "jdbc:transwarp2://10.20.149.60:31943/default";
        Connection conn = DriverManager.getConnection(jdbcURL,"hive","123456");

        Statement stmt = conn.createStatement();

        stmt.executeUpdate( "insert into user_info values(1,'zhangsan',10)" );
        stmt.executeUpdate( "insert into user_info values(2,'lisi',20)" );

        ResultSet rs = stmt.executeQuery( "select * from orc_table" );
        ResultSetMetaData metaData = rs.getMetaData();

        int columnCount = metaData.getColumnCount();
        System.out.println("column count:" + columnCount);

        for (int i=1; i<=columnCount;i++){
            System.out.println("type["+ i + "] is:" + metaData.getColumnType(i));
        }
    }

    public static void test2() throws SQLException, ClassNotFoundException{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String jdbcURL = "jdbc:transwarp2://10.20.149.60:31943/default";
        Connection conn = DriverManager.getConnection(jdbcURL,"hive","123456");

        Statement stmt = conn.createStatement();

        stmt.executeUpdate( "delete from user_info_t" );

        long startTime = System.currentTimeMillis();
        stmt.executeUpdate( "insert into user_info_t values(1,'zhangsan',10)" );
        stmt.executeUpdate( "insert into user_info_t values(2,'lisi',20)" );
        stmt.executeUpdate( "insert into user_info_t values(3,'lisi',20)" );
        stmt.executeUpdate( "insert into user_info_t values(4,'lisi',20)" );
        stmt.executeUpdate( "insert into user_info_t values(5,'lisi',20)" );
        System.out.println("spent time: " + (System.currentTimeMillis() - startTime) + "ms");

        ResultSet rs = stmt.executeQuery( "select * from user_info_t" );
        ResultSetMetaData metaData = rs.getMetaData();

        int columnCount = metaData.getColumnCount();
        System.out.println("column count:" + columnCount);

        for (int i=1; i<=columnCount;i++){
            System.out.println("type["+ i + "] is:" + metaData.getColumnType(i));
        }
    }

    public static void test3() throws SQLException, ClassNotFoundException{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String jdbcURL = "jdbc:hive2://10.20.149.60:32306/default";
        Connection conn = DriverManager.getConnection(jdbcURL,"hive","hive");

        Statement stmt = conn.createStatement();
        long startTime = System.currentTimeMillis();
    }
}
