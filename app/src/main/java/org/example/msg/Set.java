package org.example.msg;

import org.example.Node;
import java.io.Serializable;

public class Set {
    public static class SuccessMsg implements Serializable {}
    public static class FailMsg implements Serializable {}
    public static class InitiateMsg implements Serializable {
        public final int key;
        public final String value;
        public InitiateMsg(int key, String value) {
            this.key=key;
            this.value = value;
        }
    }
    public static class VersionRequestMsg implements Serializable {
        public final int key;
        public final int transacition_id;
        public VersionRequestMsg(int key, int tid) {
            this.key = key;
            this.transacition_id = tid;
        }
    }
    public static class VersionResponseMsg implements Serializable {
        public final int version;
        public final int transacition_id;
        public VersionResponseMsg(int version, int tid) {
            this.version = version;
            this.transacition_id = tid;
        }
    }
    public static class UpdateEntryMsg implements Serializable {
        public final Node.Entry entry;
        public final int key;
        public UpdateEntryMsg(int key, Node.Entry entry) {
            this.entry = entry;
            this.key = key;
        }
    }
    public static class TimeoutMsg implements Serializable {
        public final int transaction_id;
        public TimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }
}
