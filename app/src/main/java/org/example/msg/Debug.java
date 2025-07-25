package org.example.msg;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.LinkedList;

import org.example.Node.Peer;

/**
 * The class contains all the messages sent for debugging purposes.
 */
public class Debug {

    /**
     * This class represents the message to add a node in the system.
     */
    public static class AddNodeMsg implements Serializable {
        /** ActorRef of the node */
        public final ActorRef ref;
        /** ID of the node */
        public final int id;
        public AddNodeMsg(ActorRef ref, int id) {
            this.ref = ref;
            this.id = id;
        }
    }

    /**
     * Message used to tell the coordinator all of its nodes
     */
    public static class AddNodesMsg implements Serializable {
        public final LinkedList<Peer> peers;
        public AddNodesMsg(LinkedList<Peer> peers) {
            this.peers = peers;
        }
    }

    /**
     * This class represents the message to add a client to the system.
     */
    public static class AddClientMsg implements Serializable{
        /** ActorRef of the client */
        public final ActorRef ref;
        /** Name of the client */
        public final String name;
        public AddClientMsg(ActorRef ref, String name ) {
            this.ref = ref;
            this.name = name;
        }
    }

    /**
     * This class represents the message to start a simulation round.
     */
    public static class StartRoundMsg implements Serializable{ }

    /**
     * This class represents the message to increase the ongoing_action parameter of the coordinator.
     */
    public static class IncreaseOngoingMsg implements Serializable{
        /** ActorRef responsible for the increase */
        public final ActorRef responsible;

        public IncreaseOngoingMsg(ActorRef responsible) {
            this.responsible = responsible;
        }
    }

    /**
     * This class represents the message to decrease the ongoing_action parameter of the coordinator.
     */
    public static class DecreaseOngoingMsg implements Serializable{ }

    /**
     * This class represents the message to announce the coordinator to the nodes.
     */
    public static class AnnounceCoordinator implements Serializable{
        /** ActorRef of the coordinator */
        public final ActorRef coordinator;

        public AnnounceCoordinator(ActorRef coordinator) {
            this.coordinator = coordinator;
        }
    }




}
