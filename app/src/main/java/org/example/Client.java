package org.example;
import akka.actor.AbstractActor;
import akka.actor.Props;

public class Client extends AbstractActor {
    private final String name;
    public Client(String name) {
        this.name = name;
    }

    private void receiveSetSuccess(Node.SetSuccessMsg msg) {
        System.out.println(this.name + ": Success");
    }
    private void receiveSetFail(Node.SetFailMsg msg) {
        System.out.println(this.name + ": Fail");
    }

    private void receiveGetSuccess(Node.GetSuccessMsg msg) {
        System.out.println(this.name + ": Success [" + msg.key + ": " + msg.value + "]");
    }
    private void receiveGetFail(Node.GetFailMsg msg) {
        System.out.println(this.name + ": Fail ["+msg.key+"]");
    }

    static public Props props(String name) {
        return Props.create(Client.class, () -> new Client(name));
    }

	@Override
	public Receive createReceive() {
        return receiveBuilder()
        .match(Node.SetSuccessMsg.class, this::receiveSetSuccess)
        .match(Node.SetFailMsg.class, this::receiveSetFail)
        .match(Node.GetSuccessMsg.class, this::receiveGetSuccess)
        .match(Node.GetFailMsg.class, this::receiveGetFail)
        .build();
    }
}
