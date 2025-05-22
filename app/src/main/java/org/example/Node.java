package org.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {
    public final int id;
    private HashMap<Integer, Entry> storage;
    private List<Peer> peers;
    private HashMap<Integer, SetTransaction> setTransactions;
    private HashMap<Integer, GetTransaction> getTransactions;
    private int id_counter;
    private Random rnd;

    /// DEBUG

    public static class DebugAddNodeMsg implements Serializable {
        public final ActorRef ref;
        public final int id;
        public DebugAddNodeMsg(ActorRef ref, int id) {
            this.ref = ref;
            this.id = id;
        }
    }
    public void receiveDebugAddNode(DebugAddNodeMsg msg) {
        // Find the index where to put the new peer and insert it
        int i = 0;
        while (i<this.peers.size() && this.peers.get(i).id<msg.id) { i++; }
        this.peers.add(i, new Peer(msg.id, msg.ref));
    }

    private class Entry {
        public String value;
        public int version;
        public Entry (String value, int version) {
            this.value = value;
            this.version = version;
        }
    }
    private class Peer {
        public int id;
        public ActorRef ref;
        public Peer (int id, ActorRef ref) {
            this.id = id;
            this.ref = ref;
        }
    }
    private class SetTransaction {
        public final int key;
        public final String value;
        public final List<Integer> replies;
        public final ActorRef client;
        public SetTransaction(int key, String value, ActorRef client) {
            this.key = key;
            this.value = value;
            this.replies = new LinkedList<>();
            this.client = client;
        }
    }

    public Node (int id) {
        this.id = id;
        this.storage = new HashMap<>();
        this.peers = new ArrayList<>();
        this.setTransactions = new HashMap<>();
        this.getTransactions = new HashMap<>();
        this.id_counter = 0;
        this.rnd = new Random();
    }

    static public Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    /// SET(k, v)

    public static class SetSuccessMsg implements Serializable {}
    public static class SetFailMsg implements Serializable {}
    
    public static class SetMsg implements Serializable {
        public final int key;
        public final String value;
        public SetMsg(int key, String value) {
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
        public final Entry entry;
        public final int key;
        public UpdateEntryMsg(int key, Entry entry) {
            this.entry = entry;
            this.key = key;
        }
    }
    public static class SetTimeoutMsg implements Serializable {
        public final int transaction_id;
        public SetTimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }

    private List<Peer> getResponsibles(int key) {
        List<Peer> ret = new LinkedList<>();
        int i = 0;
        while (i<this.peers.size() && this.peers.get(i).id < key) {
            i++;
        }
        if (i==this.peers.size()) { i = 0; }
        for (int j=0; j<App.N; j++) {
            ret.add(this.peers.get((i+j)%this.peers.size()));
        }
        return ret;
    }

    private void receiveSet(SetMsg msg) {
        List<Peer> responsibles = this.getResponsibles(msg.key);

        this.setTransactions.put(this.id_counter, new SetTransaction(msg.key, msg.value, getSender()));
        VersionRequestMsg reqMsg = new VersionRequestMsg(msg.key, this.id_counter);

        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.SECONDS),
            getSelf(),
            new SetTimeoutMsg(this.id_counter), getContext().system().dispatcher(), getSelf());
        this.id_counter++;

        for (Peer peer: responsibles) {
            try {
                Thread.sleep(rnd.nextInt(100));
            } catch (InterruptedException e) {e.printStackTrace(); }
            peer.ref.tell(reqMsg, getSelf());
        }
    }

    private void receiveVersionRequest(VersionRequestMsg msg) {
        Entry entry = this.storage.get(msg.key);
        int version = entry==null?-1:entry.version;
        getSender().tell(new VersionResponseMsg(version, msg.transacition_id), getSelf());
    }
    private void receiveVersionResponse(VersionResponseMsg msg) {
        if (!this.setTransactions.containsKey(msg.transacition_id)) { return; }
        SetTransaction transaction = this.setTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.version);
        if (transaction.replies.size() < App.W) { return; }
        this.setTransactions.remove(msg.transacition_id);

        transaction.client.tell(new SetSuccessMsg(), getSelf());

        int maxVersion = 0;
        for (int response: transaction.replies) {
            if (response > maxVersion) {
                maxVersion = response;
            }
        }
        maxVersion++;

        List<Peer> responsibles = this.getResponsibles(msg.transacition_id);

        UpdateEntryMsg updateMsg = new UpdateEntryMsg(transaction.key, new Entry(transaction.value, maxVersion));
        for (Peer responsible: responsibles) {
            try {
                Thread.sleep(rnd.nextInt(100));
            } catch (InterruptedException e) {e.printStackTrace(); }
            responsible.ref.tell(updateMsg, getSelf());
        }
    }

    private void receiveUpdateMessage(UpdateEntryMsg msg) {
        this.storage.put(msg.key, msg.entry);
    }
    private void receiveSetTimeout(SetTimeoutMsg msg) {
        SetTransaction transaction = this.setTransactions.remove(msg.transaction_id);
        if (transaction!=null) {
            transaction.client.tell(new SetFailMsg(), getSelf());
        }
    }

    /// GET

    private class GetTransaction {
        public final int key;
        public final List<Entry> replies;
        public final ActorRef client;
        public GetTransaction(int key, ActorRef client) {
            this.key = key;
            this.replies = new LinkedList<>();
            this.client = client;
        }
    }

    public static class GetMsg implements Serializable {
        public final int key;
        public GetMsg(int key) {
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
    public static class GetTimeoutMsg implements Serializable {
        public final int transaction_id;
        public GetTimeoutMsg(int tid) {
            this.transaction_id = tid;
        }
    }

    public static class GetSuccessMsg implements Serializable {
        public final int key;
        public final String value;
        public GetSuccessMsg(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    public static class GetFailMsg implements Serializable {
        public final int key;
        public GetFailMsg(int key) {
            this.key = key;
        }
    }

    public void receiveGet(GetMsg msg) {
        List<Peer> responsibles = this.getResponsibles(msg.key);

        this.getTransactions.put(this.id_counter, new GetTransaction(msg.key, getSender()));
        EntryRequestMsg reqMsg = new EntryRequestMsg(msg.key, this.id_counter);

        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.SECONDS),
            getSelf(),
            new GetTimeoutMsg(this.id_counter), getContext().system().dispatcher(), getSelf());
        this.id_counter++;

        for (Peer peer: responsibles) {
            try {
                Thread.sleep(rnd.nextInt(100));
            } catch (InterruptedException e) {e.printStackTrace(); }
            peer.ref.tell(reqMsg, getSelf());
        }
    }

    public void receiveEntryRequest(EntryRequestMsg msg) {
        Entry entry = this.storage.get(msg.key);
        try {
            Thread.sleep(rnd.nextInt(100));
        } catch (InterruptedException e) {e.printStackTrace(); }
        getSender().tell(new EntryResponseMsg(entry, msg.transacition_id), getSelf());
    }

    public void receiveEntryResponse(EntryResponseMsg msg) {
        if (!this.getTransactions.containsKey(msg.transacition_id)) { return; }
        GetTransaction transaction = this.getTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.entry);
        if (transaction.replies.size() < App.R) { return; }
        this.getTransactions.remove(msg.transacition_id);
        
        Entry latestEntry = null;
        for (Entry entry: transaction.replies) {
            if (entry!=null && (latestEntry==null || entry.version > latestEntry.version)) {
                latestEntry = entry;
            }
        }

        transaction.client.tell(new GetSuccessMsg(transaction.key, latestEntry.value), getSelf());
    }

    public void receiveGetTimeout(GetTimeoutMsg msg) {
        GetTransaction transaction = this.getTransactions.remove(msg.transaction_id);
        if (transaction!=null) {
            transaction.client.tell(new GetFailMsg(transaction.key), getSelf());
        }
    }

	@Override
	public Receive createReceive() {
        return receiveBuilder()
        .match(SetMsg.class, this::receiveSet)
        .match(VersionRequestMsg.class, this::receiveVersionRequest)
        .match(VersionResponseMsg.class, this::receiveVersionResponse)
        .match(UpdateEntryMsg.class, this::receiveUpdateMessage)
        .match(DebugAddNodeMsg.class, this::receiveDebugAddNode)
        .match(GetMsg.class, this::receiveGet)
        .match(EntryRequestMsg.class, this::receiveEntryRequest)
        .match(EntryResponseMsg.class, this::receiveEntryResponse)
        .match(GetTimeoutMsg.class, this::receiveGetTimeout)
        .build();
	}
}
