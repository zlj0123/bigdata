package io.ibigdata.hadoop.hdfs;

public class HdfsTest {
    public static void main(String[] args) {
        String src = "100014|中债收益率曲线|575|中债商业银行同业存单收益率曲线(AAA-)||||260";

        String src1= "123|中国|123|中债||||260";

        String[] strArray = src.split("\\|");

        String[] strArray1 = src1.split("\\|");

        System.out.println();
    }
}
