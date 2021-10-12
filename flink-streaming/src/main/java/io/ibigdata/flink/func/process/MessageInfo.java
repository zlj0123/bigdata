package io.ibigdata.flink.func.process;

public class MessageInfo {
    String hostname;
    String msgTime;
    String status;/*RUNNING 正常 DEAD 宕机*/


    MessageInfo(String hostname, String msgTime, String status) {
        this.hostname = hostname;
        this.msgTime = msgTime;
        this.status = status;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getMsgTime() {
        return msgTime;
    }

    public void setMsgTime(String msgTime) {
        this.msgTime = msgTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
