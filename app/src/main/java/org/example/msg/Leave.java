package org.example.msg;

import org.example.Node.Entry;

import java.io.Serializable;
import java.util.LinkedList;

import akka.japi.Pair;


public class Leave {
    public static class InitiateMsg implements Serializable {};
    public static class AnnounceLeavingMsg implements Serializable {};
    public static class TransferItemsMsg implements Serializable {
        public final LinkedList<Pair<Integer, Entry>> items;
        public TransferItemsMsg(LinkedList<Pair<Integer, Entry>> items) {
            this.items = items;
        }
    };
}

