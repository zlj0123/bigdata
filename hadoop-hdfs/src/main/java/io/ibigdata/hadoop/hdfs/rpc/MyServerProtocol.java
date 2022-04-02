package io.ibigdata.hadoop.hdfs.rpc;

import java.io.IOException;

public interface MyServerProtocol {
    public static final long versionID = 1L;
    String echo(String value) throws IOException;
    int add(int v1,int v2) throws IOException;
}
