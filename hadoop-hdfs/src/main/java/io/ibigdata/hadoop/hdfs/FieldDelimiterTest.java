package io.ibigdata.hadoop.hdfs;

public class FieldDelimiterTest {
    public static void main(String[] args) {
        String str = "abc1 | abc2 | abc3";
        String delimiter = " \\| ";
        String[] strArray = str.split(delimiter);

        for (String str1 : strArray) {
            System.out.println(str1);
        }
    }
}
