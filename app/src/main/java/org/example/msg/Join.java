package org.example.msg;

import akka.actor.ActorRef;
import org.example.Node;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * The class represents the messages exchanged during a join operation.
 */
public class Join {
    /**
     * This class represents the message to begin a join operation.
     */
    public static class InitiateMsg implements Serializable {
        /** ActorRef of the bootstrapping peer. */
        public final ActorRef bootstrapping_peer;

        public InitiateMsg(ActorRef bootstrapping_peer) {
            this.bootstrapping_peer = bootstrapping_peer;
        }
    }

    /**
     * This class represents the message to request the network topology to the bootstrapping peer.
     */
    public static class TopologyRequestMsg implements Serializable { }

    /**
     * This class represents the message to send the network topology to the joining node.
     */
    public static class TopologyResponseMsg implements Serializable {
        /** Network topology. */
        public final List<Node.Peer> peers;

        public TopologyResponseMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }

    /**
     * This class represents the message to request the data items the joining node will be responsible for to its
     * clockwise neighbor.
     */
    public static class ResponsibilityRequestMsg implements Serializable {
        /** Id of the joining node. */
        public final int joining_id;

        public ResponsibilityRequestMsg(int joining_id) {
            this.joining_id = joining_id;
        }
    }

    /**
     * This class represents the message to give the keys of data items to the joining node will be responsible for.
     */
    public static class ResponsibilityResponseMsg implements Serializable {
        /** Set ok keys the joining node will be responsible for. */
        public final Set<Integer> keys;

        public ResponsibilityResponseMsg(Set<Integer> keys) {
            this.keys = keys;
        }
    }

    /**
     * This class represents the message to announces the joining node to other nodes.
     */
    public static class AnnouncePresenceMsg implements Serializable {
        /** Id of the joining node. */
        public final int id;

        public AnnouncePresenceMsg(int id) {
            this.id = id;
        }
    }

    /**
     * This class represents the message to stop the join operation execution because it failed.
     */
    public static class TimeoutMsg implements Serializable {}
}
