package org.example.msg;

import org.example.Node;
import org.example.Node.Entry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import akka.japi.Pair;


public class Leave {
    public static class InitiateMsg {};
    public static class AnnounceLeavingMsg {};
    public static class TransferItemsMsg {
        public final LinkedList<Pair<Integer, Entry>> items;
        public TransferItemsMsg(LinkedList<Pair<Integer, Entry>> items) {
            this.items = items;
        }
    };
}

