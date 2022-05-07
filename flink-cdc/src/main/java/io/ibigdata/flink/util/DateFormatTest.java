package io.ibigdata.flink.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static java.time.temporal.ChronoUnit.DAYS;

public class DateFormatTest {
    public static void main(String[] args) throws ParseException {
        final long DAY = 24l * 60l * 60l * 1000l; // 计算一年的毫秒数
        Date d = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date d1 = sdf.parse("1970-01-02"); // 转化为Date类型

        // 测试函数getTime()的意义，测试结果为：当Date是1970-01-01，输出是0
        //Date d2 = sdf.parse("1970-01-01");
        Date d2 = new Date(70,0,1);
        System.out.println(d2.getTime() / DAY);

        // 函数getTime()是指，到1970-01-01 00：00：00的毫秒数
        System.out.println((d1.getTime() - d2.getTime()) / DAY);
    }

}
