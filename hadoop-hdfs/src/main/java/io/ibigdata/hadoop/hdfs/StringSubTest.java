package io.ibigdata.hadoop.hdfs;

public class StringSubTest {
    public static void main(String[] args) {
        String whereClause = "where id>5";
        whereClause = whereClause.substring(6);

        System.out.println(whereClause);
    }
}
