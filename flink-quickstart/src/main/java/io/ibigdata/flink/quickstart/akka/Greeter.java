package io.ibigdata.flink.quickstart.akka;

import akka.actor.UntypedActor;

public class Greeter extends UntypedActor {
    String greeting = "";

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof WhoToGreet)
            greeting = "hello, " + ((WhoToGreet) message).who;
        else if (message instanceof Greet)
            // 发送招呼消息给发送消息给这个Actor的Actor
            getSender().tell(new Greeting(greeting), getSelf());
        else unhandled(message);
    }
}
