package org.example.msg;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import akka.japi.Pair;

import org.example.shared.*;

/**
 * The class represents the messages exchanged during a leave operation.
 */
public class Leave {
    /**
     * This class represents the message to begin a leave operation.
     */
    public static class InitiateMsg implements Serializable {};
    /**
     * This class represents the message to
     */
    public static class AnnounceLeavingMsg implements Serializable {
        public final boolean insert_staged_keys;
        public AnnounceLeavingMsg(boolean insert_staged_keys) {
            this.insert_staged_keys = insert_staged_keys;
        }
    };
    /**
     * This class represents the message to
     */
    public static class TransferItemsMsg implements Serializable {
        public final List<Pair<Integer, Entry>> items;
        public TransferItemsMsg(List<Pair<Integer, Entry>> items) {
            this.items = items;
        }
    };

    public static class AckMsg implements Serializable {};

    public static class TimeoutMsg implements Serializable {};
}

