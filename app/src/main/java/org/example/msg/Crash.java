package org.example.msg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.example.Node;
import org.example.Node.Entry;

import akka.actor.ActorRef;
import akka.japi.Pair;

public class Crash{
    public static class InitiateMsg implements Serializable {};
    public static class RecoveryMsg implements Serializable {
        public final ActorRef helper;
        public RecoveryMsg(ActorRef helper) {
            this.helper = helper;
        }
    }
    public static class TopologyRequestMsg implements Serializable {};
    public static class TopologyResponseMsg implements Serializable {
        public final List<Node.Peer> peers;
        public TopologyResponseMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }
    public static class RequestDataMsg implements Serializable {
        public final int id;
        public RequestDataMsg(int id) {
            this.id = id;
        }
    };
    public static class DataResponseMsg implements Serializable {
        public final List<Pair<Integer, Entry>> data;
        public DataResponseMsg(List<Pair<Integer, Entry>> data) {
            this.data = data;
        }
    }
}
