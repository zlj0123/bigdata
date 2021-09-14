package io.ibigdata.flink.quickstart.akka;

import java.io.Serializable;

public class WhoToGreet implements Serializable {
    public final String who;
    public WhoToGreet(String who) {
        this.who = who;
    }
}
