package org.example.msg;

import java.io.Serializable;
import org.example.shared.*;

/**
 * This class represents the messages exchanged during a set operation.
 */
public class Set {
    /** This class represents the message to communicate the success of the set request. */
    public static class SuccessMsg implements Serializable {
        public final int key;
        public SuccessMsg(int key) {
            this.key = key;
        }
    }

    /** This class represents the message to communicate that the set operation failed. */
    public static class FailMsg implements Serializable {
        public final int key;
        public FailMsg(int key) {
            this.key = key;
        }
    }

    /** This class represents the message to begin the insertion/modification of a data item in the system. */
    public static class InitiateMsg implements Serializable {
        /** Key of the data item. */
        public final int key;
        /** Value of the data item. */
        public final String value;

        public InitiateMsg(int key, String value) {
            this.key=key;
            this.value = value;
        }
    }

    /** This class represents the message to request the version of a data item. */
    public static class VersionRequestMsg implements Serializable {
        /** Key of the data item. */
        public final int key;
        /** Set request ID. */
        public final int transacition_id;

        public VersionRequestMsg(int key, int tid) {
            this.key = key;
            this.transacition_id = tid;
        }
    }

    /** This class represents the message to give the version of a data item (if contained in the storage). */
    public static class VersionResponseMsg implements Serializable {
        /** The version of the data item (or -1 if the data item isn't in the storage). */
        public final int version;
        /** Set request ID. */
        public final int transacition_id;

        public VersionResponseMsg(int version, int tid) {
            this.version = version;
            this.transacition_id = tid;
        }
    }

    /** This class represents the message to update the version of a data item or insert a new one with the right version. */
    public static class UpdateEntryMsg implements Serializable {
        /** Entry containing the version and the value of the (updated) data item. */
        public final Entry entry;
        /** Key of the data item. */
        public final int key;

        public UpdateEntryMsg(int key, Entry entry) {
            this.entry = entry;
            this.key = key;
        }
    }

    /** This class represents the message to stop the request execution. */
    public static class TimeoutMsg implements Serializable {
        /** Set request ID. */
        public final int transaction_id;

        public TimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }

    /**
     * This class represents the message to unlock a data ite key for which there were concurrent transactions
     * (write/write, read/write).
     */
    public static class UnlockMsg implements Serializable {
        /** Data item key to unlock. */
        public final int key;

        public UnlockMsg(int key) {
            this.key = key;
        }
    }
}
