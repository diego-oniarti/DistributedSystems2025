package org.example.msg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.example.Node;
import org.example.Node.Entry;

import akka.actor.ActorRef;
import akka.japi.Pair;

/**
 * This class represents the messages exchanged to make node crash and recover.
 */
public class Crash{
    /**
     * This class represents the message to begin the crash.
     */
    public static class InitiateMsg implements Serializable {};
    /**
     * This class represents the message to begin the recovery procedure.
     */
    public static class RecoveryMsg implements Serializable {
        /** Node to request lost information.
         */
        public final ActorRef helper;

        /**
         * Constructor of RecoveryMsg class.
         *
         * @param helper node to request lost information
         */
        public RecoveryMsg(ActorRef helper) {
            this.helper = helper;
        }
    }
    /**
     * This class represents the message to request the topology of the network.
     */
    public static class TopologyRequestMsg implements Serializable {};

    /**
     * This class represents the message to send the topology to the recovered node.
     */
    public static class TopologyResponseMsg implements Serializable {
        /** Topology of the network.
         */
        public final List<Node.Peer> peers;
        /**
         * Constructor of TopologyResponseMsg class.
         *
         * @param peers topology of the network
         */
        public TopologyResponseMsg(List<Node.Peer> peers) {
            this.peers = new LinkedList<Node.Peer>();
            this.peers.addAll(peers);
        }
    }
    /**
     * This class represents the message to request the data from the node that were responsible for the data items
     * of the recovered node.
     */
    public static class RequestDataMsg implements Serializable {
        /**
         * ID of the recovered node.
         */
        public final int id;
        /**
         * Constructor of RequestDataMsg class.
         *
         * @param id ID of the recovered node
         */
        public RequestDataMsg(int id) {
            this.id = id;
        }
    };
    /**
     * This class represents the message to send the data items the recovered node is responsible for.
     */
    public static class DataResponseMsg implements Serializable {
        /** List of data items.
         */
        public final List<Pair<Integer, Entry>> data;
        /**
         * Constructor of DataResponseMsg class.
         *
         * @param data list of data items
         */
        public DataResponseMsg(List<Pair<Integer, Entry>> data) {
            this.data = data;
        }
    }
}
