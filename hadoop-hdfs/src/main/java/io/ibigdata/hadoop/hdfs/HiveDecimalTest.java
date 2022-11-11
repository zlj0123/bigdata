package io.ibigdata.hadoop.hdfs;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;

import java.math.BigDecimal;

public class HiveDecimalTest {
    public static void main(String[] args) {
        BigDecimal bd = new BigDecimal("111111111111113.14159888888");
        System.out.println(bd);

        HiveDecimal hd1 = HiveDecimal.create(HiveDecimal.enforcePrecisionScale(bd,12,2));
        System.out.println(hd1);

        HiveDecimal hd2 = HiveDecimal.create("111111111111113.14159888888");
        System.out.println(hd2);

        HiveDecimal hd3 = HiveDecimalUtils.enforcePrecisionScale(HiveDecimal.create("111111111111113.14159888888"),
                12, 2);
        System.out.println(hd3);
    }
}
