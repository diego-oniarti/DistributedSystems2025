package org.example.msg;

import akka.actor.ActorRef;
import org.example.Node;
import java.io.Serializable;
import java.util.HashMap;
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
        public final ActorRef bootstrapping_peer;

        public InitiateMsg(ActorRef bootstrapping_peer) {
            this.bootstrapping_peer = bootstrapping_peer;
        }
    }
    public static class TopologyRequestMsg implements Serializable { }
    /**
     * This class represents the message to
     */
    public static class TopologyResponseMsg implements Serializable {
        public final List<Node.Peer> peers;
        public TopologyResponseMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }
    /**
     * This class represents the message to
     */
    public static class ResponsibilityRequestMsg implements Serializable {
        public final int joining_id;

        public ResponsibilityRequestMsg(int joining_id) {
            this.joining_id = joining_id;
        }
    }
    /**
     * This class represents the message to
     */
    public static class ResponsibilityResponseMsg implements Serializable {
        public final Set<Integer> keys;
        public ResponsibilityResponseMsg(Set<Integer> keys) {
            this.keys = keys;
        }
    }
    /**
     * This class represents the message to
     */
    public static class AnnouncePresenceMsg implements Serializable {
        public final int id;
        public AnnouncePresenceMsg(int id) {
            this.id = id;
        }
    }

    /**
     * This class represents the message to stop the join operation execution.
     */
    public static class TimeoutMsg implements Serializable {}
}
