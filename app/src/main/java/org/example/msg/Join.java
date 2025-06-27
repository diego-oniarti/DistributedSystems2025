package org.example.msg;

import org.example.Node;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public class Join {
    public static class InitiateMsg implements Serializable {}
    public static class TopologyMsg implements Serializable {
        public final List<Node.Peer> peers;
        public TopologyMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }
    public static class ResponsibilityRequestMsg implements Serializable {
        public final int nodeId;
        public ResponsibilityRequestMsg(int nodeId) {
            this.nodeId = nodeId;
        }
    }
    public static class ResponsibilityResponseMsg implements Serializable {
        public final HashMap<Integer, Node.Entry> responsibility;
        public ResponsibilityResponseMsg(HashMap<Integer, Node.Entry> responsibility) {
            this.responsibility = responsibility;
        }
    }

    public static class AnnouncePresenceMsg implements Serializable {
        public final int id;
        public AnnouncePresenceMsg(int id) {
            this.id = id;
        }
    }
}
