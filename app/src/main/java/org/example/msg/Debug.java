package org.example.msg;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.LinkedList;

import org.example.Node.Peer;

/**
 * The class contains all the messages sent for debugging purposes.
 */
public class Debug {
    /** This enum represents the operation's type the round is performing. */
    public static enum Ops {
        GET, SET,
        JOIN, LEAVE,
        CRASH, RECOVER
    }

    /** This class represents the message for adding a node in the system. */
    public static class AddNodeMsg implements Serializable {
        /** ActorRef of the node. */
        public final ActorRef ref;
        /** ID of the node. */
        public final int id;

        public AddNodeMsg(ActorRef ref, int id) {
            this.ref = ref;
            this.id = id;
        }
    }

    /** This class represents the message for sending the nodes in and out to the coordinator. */
    public static class AddNodesMsg implements Serializable {
        /** List of nodes in. */
        public final LinkedList<Peer> peers_in;
        /** List of nodes out. */
        public final LinkedList<Peer> peers_out;

        public AddNodesMsg(LinkedList<Peer> peers_in, LinkedList<Peer> peers_out) {
            this.peers_in = peers_in;
            this.peers_out = peers_out;
        }
    }

    /** This class represents the message for adding a client to the system. */
    public static class AddClientMsg implements Serializable{
        /** ActorRef of the client. */
        public final ActorRef ref;
        /** Name of the client. */
        public final String name;

        public AddClientMsg(ActorRef ref, String name ) {
            this.ref = ref;
            this.name = name;
        }
    }

    /** This class represents the message for starting a simulation round. */
    public static class StartRoundMsg implements Serializable{ }

    /** This class represents the message for announcing the coordinator to the nodes. */
    public static class AnnounceCoordinator implements Serializable{
        /** ActorRef of the coordinator. */
        public final ActorRef coordinator;

        public AnnounceCoordinator(ActorRef coordinator) {
            this.coordinator = coordinator;
        }
    }

    /** This class represents the message for informing the coordinator about the fail of the current round operation. */
    public static class FailMsg {
        /** Operation type. */
        public final Ops op;
        /** ActorRef of the acting node. */
        public final ActorRef node;
        /** Id of the acting node. */
        public final int id;

        public FailMsg(Ops op, int id, ActorRef node) {
            this.op = op;
            this.node = node;
            this.id = id;
        }
    }

    /** This class represents the message for informing the coordinator about the success of the current round operation. */
    public static class SuccessMsg {
        /** Operation type. */
        public final Ops op;
        /** ActorRef of the acting node. */
        public final ActorRef node;
        /** Id of the acting node. */
        public final int id;

        public SuccessMsg(Ops op, int id, ActorRef node) {
            this.op = op;
            this.node = node;
            this.id = id;
        }
    }
}
