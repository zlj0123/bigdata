package io.ibigdata.flink.test;

import org.apache.flink.client.cli.CliFrontend;

public class CliFrontendTest {
    public static void main(String[] args) {
        String[] params = new String[8];
        params[0] = "run";
        params[1] = "-t";
        params[2] = "yarn-per-job";
        params[3] = "-c";
        params[4] = "org.apache.flink.streaming.examples.socket.SocketWindowWordCount";
        params[5] = "/Users/zhanglijun/Applications/flink/examples/streaming/SocketWindowWordCount.jar";
        params[6] = "--port";
        params[7] = "9999";

        CliFrontend.main(params);
    }
}
