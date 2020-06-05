package flink.util;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySqlDataGenerator {

    public static void main(String[] args) throws Exception {
        //--------------------------------连接数据库----------------------
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.20.30.113:33061/zhanglijun";
        String user = "root";
        String password = "admin@123";

        //1、新建驱动
        Driver driverInstance = (Driver) Class.forName(driver).newInstance();
        //2、注册驱动
        DriverManager.registerDriver(driverInstance);
        //3、获取连接
        Connection conn = DriverManager.getConnection(url, user, password);

        //-----------------------------------操作数据库-----------------
        //记录开始时间
        Long begin = System.currentTimeMillis();

        //动态sql语句
        String sql = "insert into address (id,user_id,country,province,city,district,street,address) values (?,?,?,?,?,?,?,?)";
        //设置事务为非自动提交
        conn.setAutoCommit(false);
        //预编译sql
        PreparedStatement pstate = conn.prepareStatement(sql);

        for (int i = 10000; i < 20000; i++) {
            pstate.setInt(1,i);
            pstate.setInt(2,i);
            pstate.setString(3,"中国");
            pstate.setString(4,"浙江省");
            pstate.setString(5,"杭州市");
            pstate.setString(6,"西湖区");
            pstate.setString(7,"文一路");
            pstate.setString(8,i + "号");

            //添加到批处理上
            pstate.addBatch();
        }

        //批处理
        pstate.executeBatch();
        //提交
        conn.commit();

        //关闭
        pstate.close();
        conn.close();
        //结束时间
        Long end = System.currentTimeMillis();
        System.out.println("插入1万条数据，耗时:" + (end - begin) + "ms");
    }
}
