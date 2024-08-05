package io.ibigdata.hadoop.hdfs;

import java.sql.*;

public class KingbseBoolTest {
    public static String driver = "com.kingbase8.Driver";
    public static String jdbcURL = "jdbc:kingbase8://10.20.163.221:54321/test";
    public static String username = "system";
    public static String password = "admin@123";

    public static String clearSql = "truncate table boo_dest";

    public static String writeRecordSql = "INSERT INTO boo_dest (id,bool_col) VALUES(?::int4,?::bool)";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        //test1();
        test2();
    }

    public static void test1() throws SQLException, ClassNotFoundException{
        Class.forName(driver);

        try(Connection connection = DriverManager.getConnection(jdbcURL,username,password);
            Statement stmtQuery = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
        ) {
            ResultSet rs = stmtQuery.executeQuery("select id, bool_col from boo_src");
            System.out.println("id" +"      " + "bool_col");
            while (rs.next()) {
                System.out.println( rs.getString(1) + "      " + rs.getBoolean(2));
            }
        }
    }

    public static void test2() throws SQLException, ClassNotFoundException{
        Class.forName(driver);

        String metaDataSql = "select id, bool_col from boo_dest";
        try (Connection connection = DriverManager.getConnection(jdbcURL,username,password);
             Statement stmt = connection.createStatement()) {
            ResultSet metaRs = stmt.executeQuery(metaDataSql);
            ResultSetMetaData rsMetaData = metaRs.getMetaData();
            System.out.println("---------meta info----------");
            for (int i=0,len = rsMetaData.getColumnCount();i<len;i++) {
                System.out.println("column_name:" + rsMetaData.getColumnName(i+1) + "   " + "column_type:" + rsMetaData.getColumnType(i+1));
            }
        }

        try(Connection connection = DriverManager.getConnection(jdbcURL,username,password);
            Statement stmt = connection.createStatement();
            PreparedStatement preparedStatement = connection.prepareStatement(writeRecordSql);
            Statement stmtQuery = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
        ) {
            System.out.println("---------清空表----------");
            stmt.execute(clearSql);
            System.out.println("---------批插入----------");
            connection.setAutoCommit(false);

//            preparedStatement.setString(1,"2");
//            preparedStatement.setString(2,"false");
//            preparedStatement.addBatch();

            preparedStatement.setString(1,"1");
            preparedStatement.setBoolean(2,new Boolean("true"));
            preparedStatement.addBatch();

            preparedStatement.setString(1,"2");
            preparedStatement.setBoolean(2,new Boolean("false"));
            preparedStatement.addBatch();

            preparedStatement.setString(1,"3");
            preparedStatement.setBoolean(2,new Boolean(null));
            preparedStatement.addBatch();

            preparedStatement.setString(1,"4");
            preparedStatement.setString(2,"t");
            preparedStatement.addBatch();

            preparedStatement.executeBatch();
            connection.commit();

            ResultSet rs = stmtQuery.executeQuery("select id, bool_col from boo_dest");
            System.out.println("id" +"      " + "bool_col");
            while (rs.next()) {
                System.out.println( rs.getString(1) + "      " + rs.getBoolean(2));
            }
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
