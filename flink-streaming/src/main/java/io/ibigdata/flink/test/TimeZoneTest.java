package io.ibigdata.flink.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class TimeZoneTest {
    public static void main(String[] args) throws ParseException, IOException {
        //TimeZone.setDefault(TimeZone.getTimeZone("GMT+08"));
        System.out.println(TimeZone.getDefault());
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println("--------- Test 1991-09-15 00:00:00 with java.util.Date ---------");
        Date d = sf.parse("1991-09-15 00:00:00");
        System.out.println(d);

        Date dd = new Date(d.getTime());
        System.out.println("java.util.Date: " + dd);

        System.out.println("--------- Test 1991-09-15 00:00:00 with java.sql.Date ---------");
        java.sql.Date sd = new java.sql.Date(91,8,15);

        System.out.println("init java.sql.Date: " + sd);
        System.out.println("java.sql.Date: " + new java.sql.Date(new Date(sd.getTime()).getTime()));
        System.out.println("java.sql.Timestamp: " + new java.sql.Timestamp(new Date(sd.getTime()).getTime()));

        System.out.println("--------- Test 1991-09-14 00:00:00 with java.util.Date ---------");
        Date b = sf.parse("1991-09-14 00:00:00");
        System.out.println(b);

        Date bb = new Date(b.getTime());
        System.out.println("java.util.Date: " + bb);

        System.out.println("--------- Test 1991-09-14 00:00:00 with java.sql.Date ---------");
        java.sql.Date sb = new java.sql.Date(91,8,14);
        System.out.println("init java.sql.Date: " + sb);
        System.out.println("java.sql.Date: " + new java.sql.Date(new Date(sb.getTime()).getTime()));
        System.out.println("java.sql.Timestamp: " + new java.sql.Timestamp(new Date(sb.getTime()).getTime()));
    }
}
