package org.example.msg;

import org.example.Node.Entry;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import akka.japi.Pair;

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
    public static class AnnounceLeavingMsg implements Serializable {};
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

