package org.example;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import org.example.msg.*;
import org.example.msg.Debug.Ops;

/**
 * The class represents a client that makes requests to the system.
 */
public class Client extends AbstractActor {
    /** Name of the client. */
    private final String name;
    /** ActorRef of the coordinator. */
    private ActorRef coordinator;

    public Client(String name) {
        this.name = name;
    }

    /**
     * Set.SuccessMsg handler; it prints the success of a set request.
     *
     * @param msg Set.SuccessMsg message
     */
    private void receiveSetSuccess(Set.SuccessMsg msg) {
        //debug
        System.out.println(this.name + " : Success SET");

        coordinator.tell(new Debug.SuccessMsg(Ops.SET, -1, getSender()), getSelf());
    }

    /**
     * Set.FailMsg handler; it prints the fail of a set request.
     *
     * @param msg Set.FailMsg message
     */
    private void receiveSetFail(Set.FailMsg msg) {
        //debug
        System.out.println(this.name + " : Fail SET");

        coordinator.tell(new Debug.FailMsg(Ops.SET, -1, getSender()), getSelf());
    }

    /**
     * Get.SuccessMsg handler; it prints the success of a get request.
     *
     * @param msg Get.SuccessMsg message
     */
    private void receiveGetSuccess(Get.SuccessMsg msg) {
        // debug
        System.out.println(this.name + ": Success [" + msg.key + ": " + msg.value + "]");

        coordinator.tell(new Debug.SuccessMsg(Ops.GET, -1, getSender()), getSelf());
    }

    /**
     * Get.FailMsg handler; it prints the fail of a get request.
     *
     * @param msg Get.FailMsg message
     */
    private void receiveGetFail(Get.FailMsg msg) {
        // debug
        System.out.println(this.name + ": Fail ["+msg.key+"]");

        coordinator.tell(new Debug.FailMsg(Ops.GET, -1, getSender()), getSelf());
    }

    static public Props props(String name) {
        return Props.create(Client.class, () -> new Client(name));
    }

    /**
     * Debug.AnnounceCoordinator handler; the client stores the coordinator ActorRef.
     *
     * @param msg Debug.AnnounceCoordinator message
     */
    private void receiveAnnounceCoordinator(Debug.AnnounceCoordinator msg){
        this.coordinator = msg.coordinator;
    }

	@Override
	public Receive createReceive() {
        return receiveBuilder()
        .match(Set.SuccessMsg.class, this::receiveSetSuccess)
        .match(Set.FailMsg.class, this::receiveSetFail)
        .match(Get.SuccessMsg.class, this::receiveGetSuccess)
        .match(Get.FailMsg.class, this::receiveGetFail)
        .match(Debug.AnnounceCoordinator.class, this::receiveAnnounceCoordinator)
        .build();
    }
}
