package org.example;
import akka.actor.AbstractActor;

public class Client extends AbstractActor {
    private void receiveSetSuccess(Node.SetSuccessMsg msg) {
        System.out.println(getSelf().path().name() + ": Success");
    }
    private void receiveSetFail(Node.SetFailMsg msg) {
        System.out.println(getSelf().path().name() + ": Fail");
    }

	@Override
	public Receive createReceive() {
        return receiveBuilder()
        .match(Node.SetSuccessMsg.class, this::receiveSetSuccess)
        .match(Node.SetFailMsg.class, this::receiveSetFail)
        .build();
    }
}
