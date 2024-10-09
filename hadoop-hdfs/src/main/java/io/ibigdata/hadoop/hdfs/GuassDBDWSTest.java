package io.ibigdata.hadoop.hdfs;

import java.sql.*;

import static jodd.util.ThreadUtil.sleep;

public class GuassDBDWSTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        test1();
//
//        try {
//            test2();
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//
//        test1();
//
//        // 执行test2()方法两次，模拟大数据开发平台里，离线同步任务的数据预览，点击第二次，又正确了。
//        test2();
    }

    /**
     * test1()方法模拟数据源的数据预览。
     * 数据源配置的url为jdbc:gaussdb://10.20.24.29:8000/postgres?currentSchema=data_source
     * 然后在数据源的数据预览里，会传进来catalog和schema的参数，值为 postgres.data_source
     * 我们会把这个值用split(.)去分隔，得到catalog和schema，然后把上面的url反向查找最后一个斜杠(/)，
     * 把后面的值替换成catalog，url变为jdbc:gaussdb://10.20.24.29:8000/postgres，
     * 然后创建出connection，然后再执行connection.setSchema("data_source")，
     * 我们执行上面的setSchema方法后会通过connection.getSchema()得到schema，
     * 这个值永远是我们预期的实际schema，为data_source
     */

    public static void test1() throws SQLException, ClassNotFoundException{
        System.out.println("---------test1----------");
        String driver = "com.huawei.gauss200.jdbc.Driver";
        String jdbcURL = "jdbc:gaussdb://10.20.24.29:8000/postgres";
        String username = "postgres_data_source";
        String password = "123456";
        String schema = "data_source";

        Class.forName(driver);

        String metaDataSql = "select * from muti_types limit 20";
        try (Connection connection = DriverManager.getConnection(jdbcURL,username,password)) {
            connection.setSchema(schema);

            System.out.println("schema is :" + connection.getSchema());

            Statement stmt = connection.createStatement();
            ResultSet metaRs = stmt.executeQuery(metaDataSql);

            while (metaRs.next()) {
                System.out.println("get one record");
            }
        }

    }


    /**
     * test2()方法模拟大数据开发平台里，离线同步任务的数据预览。
     * 取的url为数据源里配置的jdbc url为，jdbc:gaussdb://10.20.24.29:8000/postgres?currentSchema=data_source
     * 然后他这里没有catalog和schema传递过来，直接是一个url，我们直接用这个url创建一个connection，
     * 我们也通过connection.getSchema()得到schema，并打印
     * 但这里就很奇怪了，如果单独执行test2()方法，不管执行多少次，schema正确，是我们期待的data_source。
     * 但是如果先执行test1()方法后，再执行test2()方法，发现取得的schema为pulbic，不正确，当然后面的select查询也报错了。
     */
    public static void test2() throws SQLException, ClassNotFoundException{
        System.out.println("---------test2----------");
        String driver = "com.huawei.gauss200.jdbc.Driver";
        String jdbcURL = "jdbc:gaussdb://10.20.24.29:8000/postgres?currentSchema=data_source";
        String username = "postgres_data_source";
        String password = "123456";

        Class.forName(driver);

        String metaDataSql = "select * from muti_types limit 50";
        try (Connection connection = DriverManager.getConnection(jdbcURL,username,password)) {
            System.out.println("schema is :" + connection.getSchema());

            Statement stmt = connection.createStatement();
            ResultSet metaRs = stmt.executeQuery(metaDataSql);

            while (metaRs.next()) {
                System.out.println("get one record");
            }
        }
    }
}
