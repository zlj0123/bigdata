package io.ibigdata.hadoop.hdfs;

import com.hundsun.jrescloud.common.util.EncryptUtil;

public class EncodeDecodeTest {
    public static void main(String[] args) {
        test1 ();
        test2 ();
    }

    public static void test1 () {
        String plainStr = "root";
        String enCodeStr = EncryptUtil.sm4Encode(plainStr);
        System.out.println(enCodeStr);
    }

    public static void test2 () {
        String enCodeStr = "6LFPS2cAny/IFofB3+Hyng==";
        String deCodeStr = EncryptUtil.sm4Decode(enCodeStr);
        System.out.println(deCodeStr);
    }
}
