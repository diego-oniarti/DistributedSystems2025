package org.example;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;
import scala.concurrent.duration.Duration;

import org.example.msg.*;
import org.example.msg.Join.TopologyMsg;
import org.example.msg.Leave.AnnounceLeavingMsg;
import org.example.msg.Leave.TransferItemsMsg;

public class Node extends AbstractActor {
    public final int id;
    private HashMap<Integer, Entry> storage;
    private List<Peer> peers;
    private HashMap<Integer, SetTransaction> setTransactions;
    private HashMap<Integer, GetTransaction> getTransactions;
    private int id_counter;
    private Random rnd;

    private int joiningQuorum;

    /// DEBUG

    public static class DebugAddNodeMsg implements Serializable {
        public final ActorRef ref;
        public final int id;
        public DebugAddNodeMsg(ActorRef ref, int id) {
            this.ref = ref;
            this.id = id;
        }
    }
    private void receiveDebugAddNode(DebugAddNodeMsg msg) {
        // Find the index where to put the new peer and insert it
        int i = 0;
        while (i<this.peers.size() && this.peers.get(i).id<msg.id) { i++; }
        this.peers.add(i, new Peer(msg.id, msg.ref));
    }

    // CLASSES 

    public class Entry {
        public String value;
        public int version;
        public Entry (String value, int version) {
            this.value = value;
            this.version = version;
        }
    }
    public class Peer {
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
        this.joiningQuorum = 0;
    }

    static public Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    /// SET(k, v)


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

    private void receiveSet(Set.InitiateMsg msg) {
        List<Peer> responsibles = this.getResponsibles(msg.key);

        this.setTransactions.put(this.id_counter, new SetTransaction(msg.key, msg.value, getSender()));
        Set.VersionRequestMsg reqMsg = new Set.VersionRequestMsg(msg.key, this.id_counter);

        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.SECONDS),
            getSelf(),
            new Set.TimeoutMsg(this.id_counter), getContext().system().dispatcher(), getSelf());
        this.id_counter++;

        for (Peer peer: responsibles) {
            getContext().system().scheduler().scheduleOnce(
                Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                peer.ref,
                reqMsg, getContext().system().dispatcher(),
                getSelf()
            );
        }
    }

    private void receiveVersionRequest(Set.VersionRequestMsg msg) {
        Entry entry = this.storage.get(msg.key);
        int version = entry==null?-1:entry.version;
        getSender().tell(new Set.VersionResponseMsg(version, msg.transacition_id), getSelf());
    }
    private void receiveVersionResponse(Set.VersionResponseMsg msg) {
        if (!this.setTransactions.containsKey(msg.transacition_id)) { return; }
        SetTransaction transaction = this.setTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.version);
        if (transaction.replies.size() < App.W) { return; }
        this.setTransactions.remove(msg.transacition_id);

        transaction.client.tell(new Set.SuccessMsg(), getSelf());

        int maxVersion = 0;
        for (int response: transaction.replies) {
            if (response > maxVersion) {
                maxVersion = response;
            }
        }
        maxVersion++;

        List<Peer> responsibles = this.getResponsibles(msg.transacition_id);

        Set.UpdateEntryMsg updateMsg = new Set.UpdateEntryMsg(transaction.key, new Entry(transaction.value, maxVersion));
        for (Peer responsible: responsibles) {
            getContext().system().scheduler().scheduleOnce(
                Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                responsible.ref,
                updateMsg, getContext().system().dispatcher(),
                getSelf()
            );
        }
    }

    private void receiveUpdateMessage(Set.UpdateEntryMsg msg) {
        this.storage.put(msg.key, msg.entry);
    }
    private void receiveSetTimeout(Set.TimeoutMsg msg) {
        SetTransaction transaction = this.setTransactions.remove(msg.transaction_id);
        if (transaction!=null) {
            transaction.client.tell(new Set.FailMsg(), getSelf());
        }
    }

    /// GET(k)

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

    public void receiveGet(Get.InitiateMsg msg) {
        List<Peer> responsibles = this.getResponsibles(msg.key);

        this.getTransactions.put(this.id_counter, new GetTransaction(msg.key, getSender()));
        Get.EntryRequestMsg reqMsg = new Get.EntryRequestMsg(msg.key, this.id_counter);

        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.SECONDS),
            getSelf(),
            new Get.TimeoutMsg(this.id_counter), getContext().system().dispatcher(), getSelf());
        this.id_counter++;

        for (Peer peer: responsibles) {
            getContext().system().scheduler().scheduleOnce(
                Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                peer.ref,
                reqMsg, getContext().system().dispatcher(),
                getSelf()
            );
        }
    }

    public void receiveEntryRequest(Get.EntryRequestMsg msg) {
        Entry entry = this.storage.get(msg.key);
        getContext().system().scheduler().scheduleOnce(
            Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
            getSender(),
            new Get.EntryResponseMsg(entry, msg.transacition_id), getContext().system().dispatcher(),
            getSelf()
        );
    }

    public void receiveEntryResponse(Get.EntryResponseMsg msg) {
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

        transaction.client.tell(new Get.SuccessMsg(transaction.key, latestEntry.value), getSelf());
    }

    public void receiveGetTimeout(Get.TimeoutMsg msg) {
        GetTransaction transaction = this.getTransactions.remove(msg.transaction_id);
        if (transaction!=null) {
            transaction.client.tell(new Get.FailMsg(transaction.key), getSelf());
        }
    }

    // JOIN

    private void receiveJoinInitiate(Join.InitiateMsg msg) {
        getContext().system().scheduler().scheduleOnce(
            Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
            getSender(),
            new Join.TopologyMsg(this.peers), getContext().system().dispatcher(),
            getSelf()
        );
    }
    private void receiveTopology(Join.TopologyMsg msg) {
        this.peers.addAll(msg.peers);
        List<Peer> neighbors = this.getResponsibles(this.id);
        for (Peer neighbor: neighbors) {
            getContext().system().scheduler().scheduleOnce(
                Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                neighbor.ref,
                new Join.ResponsibilityRequestMsg(this.id), getContext().system().dispatcher(),
                getSelf()
            );
        }
    }
    private void receiveResponsibilityRequest(Join.ResponsibilityRequestMsg msg) {
        // TODO: Trovare un algoritmo più elegante

        int newId = msg.nodeId;
        ActorRef newRef = getSender();

        // Get a new list of all the nodes which also contains the new one
        HashMap<Integer, Entry> ret = new HashMap<>();
        List<Peer> allNodes = new LinkedList<Peer>();
        int last_id = -1;
        for (Peer peer: peers) {
            if (last_id < newId && newId < peer.id) {
                allNodes.add(new Peer(newId, newRef));
            }
            allNodes.add(peer);
            last_id = peer.id;
        }
        if (newId > peers.getLast().id) {
            allNodes.add(new Peer(newId, newRef));
        }

        for (HashMap.Entry<Integer, Entry> dataItem: storage.entrySet()) {
            int key = dataItem.getKey();
            Entry entry = dataItem.getValue();
            
            int i = 0;
            while (i<allNodes.size() && allNodes.get(i).id < key) {
                i++;
            }
            if (i==allNodes.size()) { i = 0; }
            for (int j=0; i<App.N; j++) {
                Peer current = allNodes.get((i+j)%allNodes.size());
                if (current.ref.equals(newRef)) {
                    ret.put(key, entry);
                    break;
                }
            }

        }

        getContext().system().scheduler().scheduleOnce(
            Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
            getSender(),
            new Join.ResponsibilityResponseMsg(ret), getContext().system().dispatcher(),
            getSelf()
        );
    }
    private void receiveResponsivilityRepsonse(Join.ResponsibilityResponseMsg msg) {
        if (joiningQuorum >= App.R) {return;}
        for (HashMap.Entry<Integer, Entry> entry: msg.responsibility.entrySet()) {
            Entry currentValue = storage.get(entry.getKey());
            if (currentValue == null || currentValue.version < entry.getValue().version) {
                this.storage.put(entry.getKey(), entry.getValue());
            }
        }
        joiningQuorum++;

        if (joiningQuorum>=App.R) {
            Stream.concat(
                this.peers.stream(),
                Stream.of(new Peer(this.id, getSelf()))
            ).forEach(peer -> {
                    getContext().system().scheduler().scheduleOnce(
                        Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                        peer.ref,
                        new Join.AnnouncePresenceMsg(this.id),
                        getContext().system().dispatcher(),
                        getSelf()
                    );
                });
        }
    }

    private void ReceivePresenceAnnouncement(Join.AnnouncePresenceMsg msg) {
        // Add the new node to the list of peers
        int i=0;
        while (msg.id > this.peers.get(i).id) {
            i++;
        }
        this.peers.add(i, new Peer(msg.id, getSender()));

        // Remove the data you're no longer responsible for
        for (HashMap.Entry<Integer, Entry> entry: this.storage.entrySet()) {
            List<Peer> responsibles = this.getResponsibles(entry.getKey());
            if (!responsibles.stream().filter(p -> p.id == this.id).findFirst().isPresent()) {
                this.storage.remove(entry.getKey());
            }
        }
    }

    // LEAVE
    
    private void receiveLeave(Leave.InitiateMsg msg) {
        AnnounceLeavingMsg announcementMsg = new AnnounceLeavingMsg();
        for (Peer peer: this.peers) {
            getContext().system().scheduler().scheduleOnce(
                Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                peer.ref,
                announcementMsg,
                getContext().system().dispatcher(),
                getSelf()
            );
        }
    }

    private void receiveAnnounceLeave(Leave.AnnounceLeavingMsg msg) {
        this.peers = this.peers.stream().filter(p -> p.ref!=getSender()).collect(Collectors.toList());
        if (getSelf() != getSender()) return;

        HashMap<Peer, LinkedList<Pair<Integer, Entry>>> buckets = new HashMap<>();

        // Decide whilch elements to send to each peer
        for (HashMap.Entry<Integer, Entry> entry: this.storage.entrySet()) {
            List<Peer> newResponsibles = this.getResponsibles(entry.getKey());
            for (Peer newResponsible: newResponsibles) {
                // Create the new buckets when adding the first element to it
                if (!buckets.containsKey(newResponsible)) {
                    buckets.put(newResponsible, new LinkedList<>());
                }

                buckets.get(newResponsible).add(new Pair<>(entry.getKey(), entry.getValue()));
            }
        }

        // Send the elements
        for (HashMap.Entry<Peer, LinkedList<Pair<Integer, Entry>>> bucket: buckets.entrySet()) {
            getContext().system().scheduler().scheduleOnce(
                Duration.create(rnd.nextInt(100), TimeUnit.MILLISECONDS),
                bucket.getKey().ref,
                new TransferItemsMsg(bucket.getValue()),
                getContext().system().dispatcher(),
                getSelf()
            );
        }
    }

    private void receiveTransferItems(TransferItemsMsg msg) {
        for (Pair<Integer, Entry> dataItem: msg.items) {
            if (!this.storage.containsKey(dataItem.first()) || this.storage.get(dataItem.first()).version < dataItem.second().version) {
                this.storage.put(dataItem.first(), dataItem.second());
            }
        }
    }

	@Override
	public Receive createReceive() {
        return receiveBuilder()
        .match(Set.InitiateMsg.class, this::receiveSet)
        .match(Set.VersionRequestMsg.class, this::receiveVersionRequest)
        .match(Set.VersionResponseMsg.class, this::receiveVersionResponse)
        .match(Set.UpdateEntryMsg.class, this::receiveUpdateMessage)
        .match(DebugAddNodeMsg.class, this::receiveDebugAddNode)
        .match(Get.InitiateMsg.class, this::receiveGet)
        .match(Get.EntryRequestMsg.class, this::receiveEntryRequest)
        .match(Get.EntryResponseMsg.class, this::receiveEntryResponse)
        .match(Get.TimeoutMsg.class, this::receiveGetTimeout)
        .match(Join.InitiateMsg.class, this::receiveJoinInitiate)
        .match(Join.TopologyMsg.class, this::receiveTopology)
        .match(Join.ResponsibilityRequestMsg.class, this::receiveResponsibilityRequest)
        .match(Join.ResponsibilityResponseMsg.class, this::receiveResponsivilityRepsonse)
        .match(Join.AnnouncePresenceMsg.class, this::ReceivePresenceAnnouncement)
        .match(Leave.InitiateMsg.class, this::receiveLeave)
        .match(Leave.AnnounceLeavingMsg.class, this::receiveAnnounceLeave)
        .match(Leave.TransferItemsMsg.class, this::receiveTransferItems)
        .build();
	}
}

/**
 * Assumptions and Considerations
 * The system will start with N nodes.
 * If this were not the case, the consensum during the joining
 * operation for the first nodes wouldn't be met
 */
