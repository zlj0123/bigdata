package myflink.hbase.es.app;

import sun.misc.BASE64Decoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.TimeZone;

public class MainTest {
    public static void main(String[] args) throws ParseException, IOException {
//        String str = "江西省抚州市南城县沙洲镇珀郑焙村186号���村委会坡头村6号��院2005级学生��\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000";
//        byte[] aa = str.getBytes("UTF-8");
//        String request = new String(aa, "GBK");
//        System.out.println(request);
//        System.out.println(aa.length);
//        String str = "江西省抚州市南城县沙洲镇珀\uE07A郑焙村186号���村委会坡头村6号��院2005级学生��";
//        byte[] aa = str.getBytes("UTF-8");
//        System.out.println(aa.length);

//        BASE64Decoder decoder = new BASE64Decoder();
//        byte[] bytes = decoder.decodeBuffer("5bm/5bee5biC546v5biC5Lic6Lev77yU77yQ77yT5Y+35bm/5bee5Zu96ZmF55S15a2Q5aSn5Y6m5rCR57uE77yT77yR5Y+3k4i/5qCL77yY77yQ77yV77yN77yY77yQ77yW77yS5a6kAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
//
//
//        System.out.println(bytes.length);
//        String str1 = new String(bytes,"UTF-8");
//
//        System.out.println(str1);

        //TimeZone.setDefault(TimeZone.getTimeZone("GMT+08"));
        System.out.println(TimeZone.getDefault());
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date d = sf.parse("1991-09-15 00:00:00");
        System.out.println(d);

        Date d1 = sf.parse("1991-09-14 00:00:00");
        System.out.println(d1);
    }
}
