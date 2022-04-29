package io.ibigdata.hadoop.hdfs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.time.temporal.JulianFields;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DaysCountTest {
    public static void main(String[] args) throws ParseException {
        String value = "1970-01-01 12:00:00";

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        Date date1 = new Date();
        String value1 = dateFormat.format(date1);


        LocalDate localDateValue = LocalDate.parse(value1);
        LocalDate epoch = LocalDate.ofEpochDay(0);
        int days = (int) ChronoUnit.DAYS.between(epoch, localDateValue);

        System.out.println(days);

        // Parse date
        //SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(date1);

// Calculate Julian days and nanoseconds in the day
        LocalDate dt = LocalDate.of(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH));
        int julianDays = (int) JulianFields.JULIAN_DAY.getFrom(dt);

        System.out.println(julianDays);


        Integer timeDay = 5;
        Long time = timeDay*24*60*60*1000L;
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
        String new_date = sdf1.format(time);

        System.out.println(new_date);

    }
}
