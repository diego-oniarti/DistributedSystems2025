package org.example.msg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.example.Node;
import org.example.Node.Entry;

import akka.actor.ActorRef;
import akka.japi.Pair;

/**
 * This class represents the messages exchanged to make node crash.
 */
public class Crash{
    /**
     * This class represents the message to begin the crash.
     */
    public static class InitiateMsg implements Serializable {};
    /**
     * This class represents the message to...
     */
    public static class RecoveryMsg implements Serializable {
        public final ActorRef helper;
        public RecoveryMsg(ActorRef helper) {
            this.helper = helper;
        }
    }
    /**
     * This class represents the message to...
     */
    public static class TopologyRequestMsg implements Serializable {};
    public static class TopologyResponseMsg implements Serializable {
        public final List<Node.Peer> peers;
        public TopologyResponseMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }
    /**
     * This class represents the message to...
     */
    public static class RequestDataMsg implements Serializable {
        public final int id;
        public RequestDataMsg(int id) {
            this.id = id;
        }
    };
    /**
     * This class represents the message to...
     */
    public static class DataResponseMsg implements Serializable {
        public final List<Pair<Integer, Entry>> data;
        public DataResponseMsg(List<Pair<Integer, Entry>> data) {
            this.data = data;
        }
    }
}
