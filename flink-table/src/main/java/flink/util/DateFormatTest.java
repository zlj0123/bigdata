package flink.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatTest {
    public static void main(String[] args) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        Date date = format.parse("20200609 12:20:50");

        System.out.println(date);
    }

}
