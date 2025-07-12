package org.example.msg;

import java.io.Serializable;
import org.example.Node.Entry;

// CLASS Get -> it contains all the possible messages exchanged during a get request (read operation)
public class Get {
    /*
        CLASS InitiateMsg -> message to begin the get request
            - ATTRIBUTES
                - key -> key of the data item
     */
    public static class InitiateMsg implements Serializable {
        public final int key;
        public InitiateMsg(int key) {
            this.key=key;
        }
    }
    /*
        CLASS EntryRequestMsg -> message to request the replicas to the responsibles for the data item
            - ATTRIBUTES
                - key -> key of the data item
                - transaction_id -> id of the get request
     */
    public static class EntryRequestMsg implements Serializable {
        public final int key;
        public final int transacition_id;
        public EntryRequestMsg(int key, int tid) {
            this.key = key;
            this.transacition_id = tid;
        }
    }
    /*
        CLASS EntryResponseMsg -> message to give to the coordinator the replica
            - ATTRIBUTES
                - entry -> entry of the responsible local storage
                - transaction_id -> id of the transaction
     */
    public static class EntryResponseMsg implements Serializable {
        public final Entry entry;
        public final int transacition_id;
        public EntryResponseMsg(Entry entry, int tid) {
            this.entry = entry;
            this.transacition_id = tid;
        }
    }
    /*
        CLASS TimeoutMsg -> message to set a timeout to the get request
            - ATTRIBUTES
                - transaction_id -> id of the transaction
    */
    public static class TimeoutMsg implements Serializable {
        public final int transaction_id;
        public TimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }
    /*
        CLASS SuccessMsg -> message to notify the client about the success
            - ATTRIBUTES
                - key -> key of the data item
                - value -> value of the data item
    */
    public static class SuccessMsg implements Serializable {
        public final int key;
        public final String value;
        public SuccessMsg(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    /*
        CLASS FailMsg -> message to notify the client about the fail
            - ATTRIBUTES
                - key -> key of the data item
    */
    public static class FailMsg implements Serializable {
        public final int key;
        public FailMsg(int key) {
            this.key = key;
        }
    }
}
