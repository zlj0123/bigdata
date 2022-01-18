package io.ibigdata.flink.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatTest {
    public static void main(String[] args) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = format.format(System.currentTimeMillis());

        System.out.println(dateStr);

        System.out.println((char)('\u0014'));
        System.out.println(String.valueOf('\u0014').length());
    }

}
