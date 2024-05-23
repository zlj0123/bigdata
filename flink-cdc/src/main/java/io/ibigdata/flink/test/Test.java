package io.ibigdata.flink.test;

public class Test {
    public static void main(String[] args) {
        String str = "315324941810$407$352$000732";

        String delimiter = "\\$";

        String [] strArray = str.split(delimiter);

        System.out.println("test");
    }
}
