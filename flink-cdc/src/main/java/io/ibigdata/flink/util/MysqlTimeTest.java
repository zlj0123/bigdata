package io.ibigdata.flink.util;

import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;

public class MysqlTimeTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String url = "jdbc:mysql://10.20.30.113:33061/zhanglijun?serverTimezone=UTC";
        String user = "root";
        String password = "admin@123";

        Class.forName("com.mysql.cj.jdbc.Driver");
        DriverManager.setLoginTimeout(10000);

        Connection conn = DriverManager.getConnection(url,user,password);

        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT id, col_varchar, col_dobule, col_float, col_timestamp, col_date FROM datago_type_src t");

        ResultSetMetaData metaData = rs.getMetaData();
        int columnNumber = metaData.getColumnCount();
        while (rs.next()){
            for (int i = 1; i<= columnNumber;i++){
                String columnName = metaData.getColumnName(i);
                if (columnName.equalsIgnoreCase("col_timestamp")){
                    System.out.print(rs.getTimestamp(i) + "    ");
                }else if (columnName.equalsIgnoreCase("col_date")){
                    System.out.print(rs.getDate(i) + "    col_date");

                    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                    cal.setTime(rs.getDate(i));


                    System.out.println(cal.getTime());
                }else {
                    System.out.print(rs.getString(i) + "    ");
                }
            }

            System.out.println();
        }

    }
}
