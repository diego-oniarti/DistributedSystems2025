package org.example;
import akka.actor.AbstractActor;
import akka.actor.Props;
import org.example.msg.*;

/*
    CLASS Client -> it represents a client that makes requests to the system
        - ATTRIBUTES
            - name -> the name of the client
 */
public class Client extends AbstractActor {
    private final String name;
    public Client(String name) {
        this.name = name;
    }

    // Set.SuccessMsg handler
    private void receiveSetSuccess(Set.SuccessMsg msg) {
        System.out.println(this.name + ": Success");
    }
    // Set.FailMsg handler
    private void receiveSetFail(Set.FailMsg msg) {
        System.out.println(this.name + ": Fail");
    }
    // Get.SuccessMsg handler
    private void receiveGetSuccess(Get.SuccessMsg msg) {
        System.out.println(this.name + ": Success [" + msg.key + ": " + msg.value + "]");
    }
    // Get.FailMsg handler
    private void receiveGetFail(Get.FailMsg msg) {
        System.out.println(this.name + ": Fail ["+msg.key+"]");
    }

    static public Props props(String name) {
        return Props.create(Client.class, () -> new Client(name));
    }

	@Override
	public Receive createReceive() {
        return receiveBuilder()
        .match(Set.SuccessMsg.class, this::receiveSetSuccess)
        .match(Set.FailMsg.class, this::receiveSetFail)
        .match(Get.SuccessMsg.class, this::receiveGetSuccess)
        .match(Get.FailMsg.class, this::receiveGetFail)
        .build();
    }
}
