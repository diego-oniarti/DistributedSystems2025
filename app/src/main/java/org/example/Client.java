package org.example;
import akka.actor.AbstractActor;
import akka.actor.Props;
import org.example.msg.*;

/**
 * The class represents a client that makes requests to the system.
 */
public class Client extends AbstractActor {
    /** Name of the client. */
    private final String name;
    public Client(String name) {
        this.name = name;
    }
    /**
     * Set.SuccessMsg handler; it prints the success of a set request.
     *
     * @param msg Set.SuccessMsg message
     */
    private void receiveSetSuccess(Set.SuccessMsg msg) {
        System.out.println(this.name + " : Success");
    }
    /**
     * Set.FailMsg handler; it prints the fail of a set request.
     *
     * @param msg Set.FailMsg message
     */
    private void receiveSetFail(Set.FailMsg msg) {
        System.out.println(this.name + " : Fail");
    }
    /**
     * Get.SuccessMsg handler; it prints the success of a get request.
     *
     * @param msg Fet.SuccessMsg message
     */
    private void receiveGetSuccess(Get.SuccessMsg msg) {
        System.out.println(this.name + ": Success [" + msg.key + ": " + msg.value + "]");
    }
    /**
     * Get.FailMsg handler; it prints the fail of a get request.
     *
     * @param msg Get.FailMsg message
     */
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
