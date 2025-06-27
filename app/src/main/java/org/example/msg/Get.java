package org.example.msg;

import java.io.Serializable;
import org.example.Node.Entry;

public class Get {
    public static class InitiateMsg implements Serializable {
        public final int key;
        public InitiateMsg(int key) {
            this.key=key;
        }
    }
    public static class EntryRequestMsg implements Serializable {
        public final int key;
        public final int transacition_id;
        public EntryRequestMsg(int key, int tid) {
            this.key = key;
            this.transacition_id = tid;
        }
    }
    public static class EntryResponseMsg implements Serializable {
        public final Entry entry;
        public final int transacition_id;
        public EntryResponseMsg(Entry entry, int tid) {
            this.entry = entry;
            this.transacition_id = tid;
        }
    }
    public static class TimeoutMsg implements Serializable {
        public final int transaction_id;
        public TimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }

    public static class SuccessMsg implements Serializable {
        public final int key;
        public final String value;
        public SuccessMsg(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    public static class FailMsg implements Serializable {
        public final int key;
        public FailMsg(int key) {
            this.key = key;
        }
    }
}
