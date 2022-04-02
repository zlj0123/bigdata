package io.ibigdata.hadoop.hdfs.rpc;

import java.io.IOException;

public class MyServerProtocolImpl implements MyServerProtocol {
    @Override
    public String echo(String value) throws IOException {
        return value;
    }
    @Override
    public int add(int v1, int v2) throws IOException {
        return v1 + v2;
    }

}
