package org.example.msg;

import java.io.Serializable;
import org.example.shared.*;

/**
 * This class represents the messages exchanged during a get request (read operation).
 */
public class Get {
    /**
     * This class represents the message to begin a get request.
     */
    public static class InitiateMsg implements Serializable {
        /** Key of the data item.
         */
        public final int key;

        public InitiateMsg(int key) {
            this.key=key;
        }
    }
    /**
     * This class represents the message to request the replicas to the responsibles for the data item.
     */
    public static class EntryRequestMsg implements Serializable {
        /** Key of the data item.
         */
        public final int key;
        /** Get request ID.
         */
        public final int transacition_id;

        public EntryRequestMsg(int key, int tid) {
            this.key = key;
            this.transacition_id = tid;
        }
    }
    /**
     * This class represents the message to to give to the coordinator the replica.
     */
    public static class EntryResponseMsg implements Serializable {
        /** Entry of the responsible local storage.
         */
        public final Entry entry;
        /** Get request ID.
         */
        public final int transacition_id;

        public EntryResponseMsg(Entry entry, int tid) {
            this.entry = entry;
            this.transacition_id = tid;
        }
    }
    /**
     * This class represents the message to set a timeout to the get request.
     */
    public static class TimeoutMsg implements Serializable {
        /** Get request ID.
         */
        public final int transaction_id;

        public TimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }
    /**
     * This class represents the message to notify the client about the success.
     */
    public static class SuccessMsg implements Serializable {
        /** Key of the data item.
         */
        public final int key;
        /** Value of the data item.
         */
        public final String value;
        public final int version;

        public SuccessMsg(int key, String value, int version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }
    }
    /**
     * This class the message to notify the client about the fail.
     */
    public static class FailMsg implements Serializable {
        /** Key of the data item.
         */
        public final int key;

        public FailMsg(int key) {
            this.key = key;
        }
    }
}
