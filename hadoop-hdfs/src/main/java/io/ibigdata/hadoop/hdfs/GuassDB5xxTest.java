package io.ibigdata.hadoop.hdfs;

import java.io.ByteArrayInputStream;
import java.sql.*;

public class GuassDB5xxTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test1();
        // test2();
    }

    public static void test1() throws SQLException, ClassNotFoundException{
        String driver = "com.huawei.gaussdb.jdbc.Driver";
        String jdbcURL = "jdbc:gaussdb://10.20.194.39:8000/uf30";
        String username = "hs_fil";
        String password = "UF30.hundsun";

        Class.forName(driver);

        String metaDataSql = "select c_tinyint, c_smallint, c_bigint, c_int ,c_number, c_boolean, c_blob, c_bytes, c_bit from datago_dest_types where 1=2";
        try (Connection connection = DriverManager.getConnection(jdbcURL,username,password);
             Statement stmt = connection.createStatement()) {
            ResultSet metaRs = stmt.executeQuery(metaDataSql);
            ResultSetMetaData rsMetaData = metaRs.getMetaData();
            System.out.println("---------meta info----------");
            for (int i=0,len = rsMetaData.getColumnCount();i<len;i++) {
                System.out.println("column_name:" + rsMetaData.getColumnName(i+1) + "   " + "column_type:" + rsMetaData.getColumnType(i+1));
            }
        }

        String writeRecordSql = "INSERT INTO datago_dest_types (" +
                "c_tinyint, c_smallint, c_bigint, c_int ,c_number, c_boolean, c_blob, c_bytes, c_bit) values (?::int1,?::int2,?::int4,?::int4,?::numeric,?::boolean,?::blob,?::bytea,?::bit varying)";

        try(Connection connection = DriverManager.getConnection(jdbcURL,username,password);
            PreparedStatement preparedStatement = connection.prepareStatement(writeRecordSql);
        ) {
            System.out.println("---------批插入----------");
            connection.setAutoCommit(false);

            preparedStatement.setString(1,"1");
            preparedStatement.setString(2,"1");
            preparedStatement.setString(3,"1");
            preparedStatement.setString(4,"4");
            preparedStatement.setString(5,"12345678911.1234560022");
            preparedStatement.setString(6,"0");

            byte[] bytes = {1, 2, 3, 4,5,6,7,8,9,1,2,3,4,5,6,7};
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

//            Blob blob = preparedStatement.getConnection().createBlob();
//            blob.setBytes(1,null);
//            preparedStatement.setBlob(7,blob);
//            preparedStatement.setBytes(7, null);
//            preparedStatement.setNull(7,Types.BLOB);
//            preparedStatement.setBytes(7, bytes);
            preparedStatement.setBlob(7, inputStream);

            preparedStatement.setBytes(8,bytes);
            preparedStatement.setString(9,"101");

            preparedStatement.addBatch();

            preparedStatement.setString(1,"1");
            preparedStatement.setString(2,"1");
            preparedStatement.setString(3,"1");
            preparedStatement.setString(4,"5");
            preparedStatement.setString(5,"12345678911.1234560022");
            preparedStatement.setString(6,"0");

//            Blob blob2 = preparedStatement.getConnection().createBlob();
//            blob2.setBytes(1,null);
//            preparedStatement.setBlob(7,blob2);
//            preparedStatement.setBytes(7, null);
//            preparedStatement.setNull(7,Types.BLOB);
//            preparedStatement.setBytes(7, bytes);
            preparedStatement.setBlob(7, inputStream);

            preparedStatement.setBytes(8,bytes);
            preparedStatement.setString(9,"101");

            preparedStatement.addBatch();

            preparedStatement.executeBatch();
            connection.commit();
        }
    }


    public static void test2() throws SQLException, ClassNotFoundException{
        String driver = "com.huawei.gaussdb.jdbc.Driver";
        String jdbcURL = "jdbc:gaussdb://10.20.194.39:8000/uf30";
        String username = "hs_fil";
        String password = "UF30.hundsun";

        Class.forName(driver);

        String metaDataSql = "select c_tinyint, c_smallint, c_bigint, c_int ,c_number, c_boolean, c_blob, c_bytes, c_bit from datago_source_types";
        try (Connection connection = DriverManager.getConnection(jdbcURL,username,password);
             Statement stmt = connection.createStatement()) {
            ResultSet metaRs = stmt.executeQuery(metaDataSql);
            ResultSetMetaData rsMetaData = metaRs.getMetaData();
            System.out.println("---------meta info----------");
            for (int i=0,len = rsMetaData.getColumnCount();i<len;i++) {
                System.out.println("column_name:" + rsMetaData.getColumnName(i+1) + "   " + "column_type:" + rsMetaData.getColumnType(i+1));
            }

            while (metaRs.next()) {
                byte[] bytes = metaRs.getBytes(7);

                System.out.println(bytes);
            }
        }
    }
}
