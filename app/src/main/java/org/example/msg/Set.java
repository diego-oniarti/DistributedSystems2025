package org.example.msg;

import org.example.Node;
import java.io.Serializable;

// CLASS Set -> contains all the messages for the set operation
public class Set {
    // CLASS SuccessMsg -> message to communicate that the set operation succeed
    public static class SuccessMsg implements Serializable {}
    // CLASS FailMsg -> message to communicate that the set operation failed
    public static class FailMsg implements Serializable {}
    /*
        CLASS InitiateMsg -> message to begin the insertion/modification of a data item in the system
            - ATTRIBUTES
                - key -> the key of the data item
                - value -> the value of the data item
     */
    public static class InitiateMsg implements Serializable {
        public final int key;
        public final String value;
        public InitiateMsg(int key, String value) {
            this.key=key;
            this.value = value;
        }
    }
    /*
        CLASS VersionRequestMsg -> message to request the version of a data item
            - ATTRIBUTES
                - key -> key of the data item
                - transaction_id -> id of the set request
     */
    public static class VersionRequestMsg implements Serializable {
        public final int key;
        public final int transacition_id;
        public VersionRequestMsg(int key, int tid) {
            this.key = key;
            this.transacition_id = tid;
        }
    }
    /*
        CLASS VersionResponseMsg -> message to give the version of a data item (if contained in the storage)
            - ATTRIBUTES
                - version -> the version of the data item (or -1 if the data item isn't in the storage)
                - transaction_id -> id of the set request
     */
    public static class VersionResponseMsg implements Serializable {
        public final int version;
        public final int transacition_id;
        public VersionResponseMsg(int version, int tid) {
            this.version = version;
            this.transacition_id = tid;
        }
    }
    /*
    CLASS UpdateEntryMsg -> message to update the version of a data item or insert a new one with the right version
        - ATTRIBUTES
            - key -> key of the data item
            - entry -> entry containing the version and the value of the (updated) data item
    */
    public static class UpdateEntryMsg implements Serializable {
        public final Node.Entry entry;
        public final int key;
        public UpdateEntryMsg(int key, Node.Entry entry) {
            this.entry = entry;
            this.key = key;
        }
    }
    /*
    CLASS TimeoutMsg
        - ATTRIBUTES
            - transaction_id -> id of the set request
    */
    public static class TimeoutMsg implements Serializable {
        public final int transaction_id;
        public TimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }
}
