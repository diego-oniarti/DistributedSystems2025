package org.example.msg;

import java.io.Serializable;
import java.util.List;

import akka.japi.Pair;

import org.example.shared.*;

/**
 * The class represents the messages exchanged during a leave operation.
 */
public class Leave {
    /** This class represents the message to begin a leave operation. */
    public static class InitiateMsg implements Serializable {};

    /** This class represents the message to announce that the leaving node is leaving. */
    public static class AnnounceLeavingMsg implements Serializable {
        /** True if other nodes need to transfer items to the staged storage to their actual storage. */
        public final boolean insert_staged_keys;

        public AnnounceLeavingMsg(boolean insert_staged_keys) {
            this.insert_staged_keys = insert_staged_keys;
        }
    };

    /** This class represents the message to give the leaving node items to their new responsible nodes. */
    public static class TransferItemsMsg implements Serializable {
        /** List of data items to insert in the staged storage. */
        public final List<Pair<Integer, Entry>> items;

        public TransferItemsMsg(List<Pair<Integer, Entry>> items) {
            this.items = items;
        }
    };

    /** This class represents the message to inform the leaving node that the new responsible stored its data items. */
    public static class AckMsg implements Serializable {};

    /** This class represents the message to fail the leaving operation. */
    public static class TimeoutMsg implements Serializable {};
}

