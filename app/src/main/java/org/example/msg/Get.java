package org.example.msg;

import java.io.Serializable;
import org.example.Node.Entry;

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
        /**
         * Constructor of IntiateMsg class.
         *
         * @param key key  of the data item.
         */
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
        /**
         * Constructor of EntryRequestMsg class.
         *
         * @param key key of the data item.
         * @param tid get request ID
         */
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
        /**
         * Constructor of EntryResponseMsg class.
         *
         * @param entry entry of the responsible local storage
         * @param tid get request ID
         */
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

        /**
         * Constructor of TimeoutMsg class.
         *
         * @param tid get request ID
         */
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

        /**
         * Constructor of SuccessMsg class.
         *
         * @param key key of the data item
         * @param value value of the data item
         */
        public SuccessMsg(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    /**
     * This class the message to notify the client about the fail.
     */
    public static class FailMsg implements Serializable {
        /** Key of the data item.
         */
        public final int key;

        /**
         * Constructor of FailMsg class.
         *
         * @param key key of the data item.
         */
        public FailMsg(int key) {
            this.key = key;
        }
    }
}
