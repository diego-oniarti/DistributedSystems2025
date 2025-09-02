package org.example.msg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.example.Node;
import org.example.shared.*;

import akka.actor.ActorRef;
import akka.japi.Pair;

/**
 * This class represents the messages exchanged during a crash/recovery operation.
 */
public class Crash{
    /** This class represents the message for beginning the crash. */
    public static class InitiateMsg implements Serializable {};

    /** This class represents the message for beginning the recovery procedure. */
    public static class RecoveryMsg implements Serializable {
        /** Node to request lost information. */
        public final ActorRef helper;

        public RecoveryMsg(ActorRef helper) {
            this.helper = helper;
        }
    }

    /** This class represents the message for requesting the topology of the network. */
    public static class TopologyRequestMsg implements Serializable {};

    /** This class represents the message for giving the topology to the recovered node. */
    public static class TopologyResponseMsg implements Serializable {
        /** Network topology. */
        public final List<Node.Peer> peers;

        public TopologyResponseMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }

    /**
     * This class represents the message for requesting the data to the node that were responsible for the data items
     * of the recovered node.
     */
    public static class RequestDataMsg implements Serializable {
        /** ID of the recovered node. */
        public final int id;

        public RequestDataMsg(int id) {
            this.id = id;
        }
    };

    /** This class represents the message for giving the data items the recovered node is responsible for. */
    public static class DataResponseMsg implements Serializable {
        /** List of data items. */
        public final List<Pair<Integer, Entry>> data;

        public DataResponseMsg(List<Pair<Integer, Entry>> data) {
            this.data = data;
        }
    }
}
