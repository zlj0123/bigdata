package io.ibigdata.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.sql.*;

public class Opengauss5And8Test {
    public static String DRIVER_CLASS_NAME = "org.opengauss.Driver";

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test5();
        test8();
    }

    public static void test5() throws SQLException, ClassNotFoundException{
        String jdbcURL = "jdbc:opengauss://10.20.148.120:5432/datago";
        String username = "datago";
        String password = "datago";

        String clearSql = "truncate table datago_test1";
        String writeRecordSql = "INSERT INTO datago_test1 (c_int,c_tinyint,c_smallint,c_integer,c_bigint,c_number,c_boolean,c_clob,c_blob,c_byte) VALUES(?,?,?,?,?,?,?,?,?,?)";
        String metaDataSql = "select c_int,c_tinyint,c_smallint,c_integer,c_bigint,c_number,c_boolean,c_clob,c_blob,c_byte from datago_test1 where 1=2";

        Class.forName(DRIVER_CLASS_NAME);

        try(Connection connection = DriverManager.getConnection(jdbcURL,username,password);
            Statement stmt = connection.createStatement();
            PreparedStatement preparedStatement = connection.prepareStatement(writeRecordSql);
            Statement stmtQuery = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
        ) {

            ResultSet metaRs = stmt.executeQuery(metaDataSql);
            ResultSetMetaData rsMetaData = metaRs.getMetaData();
            System.out.println("---------meta info----------");
            for (int i=0,len = rsMetaData.getColumnCount();i<len;i++) {
                System.out.println("column_name:" + rsMetaData.getColumnName(i+1) + "   " + "column_type:" + rsMetaData.getColumnType(i+1));
            }

            System.out.println("---------清空表----------");
            stmt.execute(clearSql);
            System.out.println("---------批插入----------");
            connection.setAutoCommit(false);

            preparedStatement.setString(1,"1");
            preparedStatement.setNull(2,Types.INTEGER);
            preparedStatement.setInt(3,1);
            preparedStatement.setString(4,"1");
            preparedStatement.setString(5,"223372036854775807");
            preparedStatement.setString(6,"12345678911.123456789");
            preparedStatement.setBoolean(7,Boolean.TRUE);
            preparedStatement.setString(8,"abcdefghhhhhh.123456789h");

            byte[] bytes = {1, 2, 3, 4};
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            preparedStatement.setBlob(9, inputStream);
            preparedStatement.setBytes(10,bytes);
            preparedStatement.addBatch();

            preparedStatement.setString(1,"2");
            preparedStatement.setInt(2,2);
            preparedStatement.setInt(3,2);
            preparedStatement.setString(4,"2");
            preparedStatement.setString(5,"223372036854775807");
            preparedStatement.setString(6,"12345678911.123456789");
            preparedStatement.setBoolean(7,Boolean.FALSE);
            preparedStatement.setString(8,"abcdefghhhhhh.123456789h");

            ByteArrayInputStream inputStream1 = new ByteArrayInputStream(bytes);
            preparedStatement.setNull(9, Types.BLOB);
            preparedStatement.setBytes(10,bytes);
            preparedStatement.addBatch();

            preparedStatement.executeBatch();
            connection.commit();

            ResultSet rs = stmtQuery.executeQuery("select c_int,c_tinyint,c_smallint,c_integer,c_bigint,c_number,c_boolean,c_clob,c_blob,c_byte from datago_test1");
            System.out.println("---------------打印数据----------------------");
            while (rs.next()) {
                System.out.println( rs.getObject(1) + "      " + rs.getObject(2) + "      " + rs.getObject(3)
                        + "      " + rs.getObject(4) + "      " + rs.getObject(5) + "      " + rs.getObject(6)
                        + "      " + rs.getObject(7) + "      " + rs.getObject(8) + "      " + rs.getObject(9));
            }
        }
    }

    public static void test8() throws SQLException, ClassNotFoundException{
        String jdbcURL = "jdbc:opengauss://10.20.194.39:8000/dbtrade";
        String username = "dbtrade";
        String password = "dbtrade123!";

        String clearSql = "truncate table datago_test1";
        String writeRecordSql = "INSERT INTO datago_test1 (c_int,c_tinyint,c_smallint,c_integer,c_bigint,c_number,c_boolean,c_clob,c_blob,c_byte) VALUES(?,?,?,?,?,?,?,?,?,?)";
        String metaDataSql = "select c_int,c_tinyint,c_smallint,c_integer,c_bigint,c_number,c_boolean,c_clob,c_blob,c_byte from datago_test1 where 1=2";

        Class.forName(DRIVER_CLASS_NAME);

        try(Connection connection = DriverManager.getConnection(jdbcURL,username,password);
            Statement stmt = connection.createStatement();
            PreparedStatement preparedStatement = connection.prepareStatement(writeRecordSql);
            Statement stmtQuery = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
        ) {

            ResultSet metaRs = stmt.executeQuery(metaDataSql);
            ResultSetMetaData rsMetaData = metaRs.getMetaData();
            System.out.println("---------meta info----------");
            for (int i=0,len = rsMetaData.getColumnCount();i<len;i++) {
                System.out.println("column_name:" + rsMetaData.getColumnName(i+1) + "   " + "column_type:" + rsMetaData.getColumnType(i+1));
            }

            System.out.println("---------清空表----------");
            stmt.execute(clearSql);
            System.out.println("---------批插入----------");
            connection.setAutoCommit(false);

            preparedStatement.setString(1,"1");
            preparedStatement.setNull(2,Types.INTEGER);
            preparedStatement.setInt(3,1);
            // preparedStatement.setInt(4,1);
            preparedStatement.setString(4,"1");
            preparedStatement.setString(5,"223372036854775807");
            preparedStatement.setString(6,"12345678911.123456789");
            preparedStatement.setBoolean(7,Boolean.TRUE);
            preparedStatement.setString(8,"abcdefghhhhhh.123456789h");

            byte[] bytes = {1, 2, 3, 4};
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            preparedStatement.setBlob(9, inputStream);
            preparedStatement.setBytes(10,bytes);
            preparedStatement.addBatch();

            preparedStatement.setString(1,"2");
            preparedStatement.setInt(2,2);
            preparedStatement.setInt(3,2);
            // preparedStatement.setInt(4,2);
            preparedStatement.setString(4,"2");
            preparedStatement.setString(5,"223372036854775807");
            preparedStatement.setString(6,"12345678911.123456789");
            preparedStatement.setBoolean(7,Boolean.FALSE);
            preparedStatement.setString(8,"abcdefghhhhhh.123456789h");

            ByteArrayInputStream inputStream1 = new ByteArrayInputStream(bytes);
            preparedStatement.setNull(9, Types.BLOB);
            preparedStatement.setBytes(10,bytes);
            preparedStatement.addBatch();

            preparedStatement.executeBatch();
            connection.commit();

            ResultSet rs = stmtQuery.executeQuery("select c_int,c_tinyint,c_smallint,c_integer,c_bigint,c_number,c_boolean,c_clob,c_blob,c_byte from datago_test1");
            System.out.println("---------------打印数据----------------------");
            while (rs.next()) {
                System.out.println( rs.getObject(1) + "      " + rs.getObject(2) + "      " + rs.getObject(3)
                        + "      " + rs.getObject(4) + "      " + rs.getObject(5) + "      " + rs.getObject(6)
                        + "      " + rs.getObject(7) + "      " + rs.getObject(8) + "      " + rs.getObject(9));
            }
        }
    }
}
