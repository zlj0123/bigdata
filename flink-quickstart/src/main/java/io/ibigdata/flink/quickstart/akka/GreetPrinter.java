package io.ibigdata.flink.quickstart.akka;

import akka.actor.UntypedActor;

public class GreetPrinter extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof Greeting)
            System.out.println(((Greeting) message).message);
    }
}
