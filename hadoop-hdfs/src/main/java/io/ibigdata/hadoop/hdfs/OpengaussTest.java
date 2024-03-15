package io.ibigdata.hadoop.hdfs;

import java.sql.*;

public class OpengaussTest {
    public static String driver = "org.opengauss.Driver";
    public static String jdbcURL = "jdbc:opengauss://10.20.148.120:5432/datago";
    public static String username = "datago";
    public static String password = "datago";

    public static String clearSql = "truncate table boo_dest";

    public static String writeRecordSql = "INSERT INTO boo_dest (id,bool_col) VALUES(?,?)";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test1();
    }

    public static void test1() throws SQLException, ClassNotFoundException{
        Class.forName(driver);

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

            preparedStatement.setString(1,"1");
            preparedStatement.setString(2,"true");
            preparedStatement.addBatch();

            preparedStatement.setString(1,"2");
            preparedStatement.setString(2,"false");
            preparedStatement.addBatch();

//            preparedStatement.setString(1,"1");
//            preparedStatement.setBoolean(2,new Boolean("true"));
//            preparedStatement.addBatch();
//
//            preparedStatement.setString(1,"2");
//            preparedStatement.setBoolean(2,new Boolean("false"));
//            preparedStatement.addBatch();
//
//            preparedStatement.setString(1,"3");
//            preparedStatement.setBoolean(2,new Boolean(null));
//            preparedStatement.addBatch();

            preparedStatement.executeBatch();
            connection.commit();

            ResultSet rs = stmtQuery.executeQuery("select id, bool_col from boo_dest");
            System.out.println("id" +"      " + "bool_col");
            while (rs.next()) {
                System.out.println( rs.getString(1) + "      " + rs.getBoolean(2));
            }
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
