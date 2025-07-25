package org.example.msg;

import akka.actor.ActorRef;
import org.example.Node;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * The class represents the messages exchanged during a join operation.
 */
public class Join {
    /**
     * This class represents the message to begin a join operation.
     */
    public static class InitiateMsg implements Serializable { }
    /**
     * This class represents the message to
     */
    public static class TopologyMsg implements Serializable {
        public final List<Node.Peer> peers;
        public TopologyMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }
    /**
     * This class represents the message to
     */
    public static class ResponsibilityRequestMsg implements Serializable {
        public final int nodeId;
        public ResponsibilityRequestMsg(int nodeId) {
            this.nodeId = nodeId;
        }
    }
    /**
     * This class represents the message to
     */
    public static class ResponsibilityResponseMsg implements Serializable {
        public final HashMap<Integer, Node.Entry> responsibility;
        public ResponsibilityResponseMsg(HashMap<Integer, Node.Entry> responsibility) {
            this.responsibility = responsibility;
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
}
