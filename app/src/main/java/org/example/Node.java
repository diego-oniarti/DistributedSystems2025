package org.example;

import static org.example.App.MSG_MAX_DELAY;
import static org.example.App.N;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.Pair;
import org.example.msg.Set;
import org.example.msg.Debug.Ops;
import org.example.msg.Leave.AnnounceLeavingMsg;

import org.example.shared.Entry;

import scala.concurrent.duration.Duration;

import org.example.msg.*;

/**
 * The class node represents a node in the system.
 */
public class Node extends AbstractActor {

    /// ATTRIBUTES

    /** ID of the node. */
    public final int id;
    /** Contains the data items associated with the node. */
    private HashMap<Integer, Entry> storage;
    /** List of all the nodes in the network. */
    private List<Peer> peers;
    /** List of set requests (write) that the node is managing. */
    private HashMap<Integer, SetTransaction> setTransactions;
    /** List of get requests (read) that the node is managing. */
    private HashMap<Integer, GetTransaction> getTransactions;
    /** Number of transactions that the node is managing. */
    private int id_counter;

    /** Boolean value, if true the node crashed. */
    private boolean crashed;

    /** Number of data items the joining node is reading */
    private int joinKeyCount;
    /** True if the joining node was able to contact the bootstrapping peer */
    private boolean is_joining;
    /** True if one or more joining node read operations failed */
    private boolean join_failed;

    /** Map containing the data items of the leaving node that will be put in the storage if the operation is successful */
    private HashMap<Integer, Entry> stagedStorage;
    /** Number of nodes that will store leaving node data items */
    private int leavingCount;
    /** True is the leaving node is leaving (it is sending its data items to other nodes) */
    private boolean is_leaving;

    /** Set of data items keys for which the node is executing set transactions */
    private java.util.Set<Integer> ongoing_set_keys;

    /** ActorRef of the coordinator */
    private ActorRef coordinator;

    private Random rnd;

    /// UTILS

    /**
     * Schedules a message to be sent to a target with a random delay.
     *
     * @param target {@link ActorRef} actor that will receive the message
     * @param msg {@link Serializable} any message
     * @param sender {@link ActorRef} actor sendig the message
     * @param duration int maximum amount of milliseconds the message will take to propagate
     */
    private void sendMessageDelay(ActorRef target, Serializable msg, ActorRef sender, int duration) {
        getContext().system().scheduler().scheduleOnce(
            Duration.create(rnd.nextInt(duration), TimeUnit.MILLISECONDS),
            target,
            msg, getContext().system().dispatcher(),
            sender
        );
    }

    /** Overload for {@link #sendMessageDelay(ActorRef, Serializable, ActorRef, int)} with default duration. */
    private void sendMessageDelay(ActorRef target, Serializable msg, ActorRef sender) {
        sendMessageDelay(target, msg, sender, MSG_MAX_DELAY);
    }

    /** Overload for {@link #sendMessageDelay(ActorRef, Serializable, ActorRef, int)} with default duration and sender. */
    private void sendMessageDelay(ActorRef target, Serializable msg) {
        sendMessageDelay(target, msg, getSelf(), MSG_MAX_DELAY);
    }

    /**
     * It schedules a message in the future.
     *
     * @param msg a serializable object (a message)
     */
    private void setTimeout(Serializable msg) {
        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.MILLISECONDS),
            getSelf(),
            msg, getContext().system().dispatcher(),
            getSelf()
        );
    }

    /**
     * Method to determine if a node is responsible for a data item.
     *
     * @param key key of the data item
     * @param node Actor reference of the node of interest
     * @return true if the node is responsible for the data item, false otherwise
     */
    private boolean isResponsible(int key, ActorRef node) {
        List<Peer> peers = this.getResponsibles(key);
        return peers.stream().anyMatch(p->p.ref==node);
    }

    /**
     * Discovers the nodes that are responsible for a specific data item.
     *
     * @param key key of the data item
     * @return the list of nodes that are responsibles for the data item
     */
    private List<Peer> getResponsibles(int key) {
        List<Peer> ret = new LinkedList<>();
        int i = 0;
        // increase the index until we reach the end of the list, or we find the peer
        // with an id that is major than the data item key
        while (i<this.peers.size() && this.peers.get(i).id < key) {
            i++;
        }
        // if we reached the end, we start from the beginning
        if (i==this.peers.size()) { i = 0; }
        // collect the N peers that are responsible for the data item (we treat the
        // list like a "circular vector")
        for (int j = 0; j< N; j++) {
            ret.add(this.peers.get((i+j)%this.peers.size()));
        }
        return ret;
    }

    /// DEBUG

    private void receiveDebugAddNode(Debug.AddNodeMsg msg) {
        // Find the index where to put the new peer and insert it
        int i = 0;
        while (i<this.peers.size() && this.peers.get(i).id<msg.id) { i++; }
        this.peers.add(i, new Peer(msg.id, msg.ref));
    }

    private void receiveAnnounceCoordinator(Debug.AnnounceCoordinator msg){
        this.coordinator = msg.coordinator;
    }

    /// CLASSES

    /** The class represents a peer in the network. */
    public static class Peer {
        /** ID of the peer. */
        public int id;
        /** Actor reference of the peer. */
        public ActorRef ref;

        public Peer (int id, ActorRef ref) {
            this.id = id;
            this.ref = ref;
        }
    }

    /** The class represents a set request (adding or updating a data item). */
    private class SetTransaction {
        /** Key of data item. */
        public final int key;
        /** Value of the data item. */
        public final String value;
        /** List of versions of the collected replies. */
        public final List<Integer> replies;
        /** Actor reference of the client that made the set request. */
        public final ActorRef client;

        public SetTransaction(int key, String value, ActorRef client) {
            this.key = key;
            this.value = value;
            this.replies = new LinkedList<>();
            this.client = client;
        }
    }

    /** The class represents a get request (read a data item). */
    private class GetTransaction {
        /** Key of the data item. */
        public final int key;
        /** List of entries of the collected replies. */
        public final List<Entry> replies;
        /** Actor reference of the client that made the get request. */
        public final ActorRef client;

        public GetTransaction(int key, ActorRef client) {
            this.key = key;
            this.replies = new LinkedList<>();
            this.client = client;
        }
    }

    /// CONSTRUCTOR

    public Node (int id) {
        this.id = id;
        this.storage = new HashMap<>();
        this.peers = new ArrayList<>();
        this.setTransactions = new HashMap<>();
        this.getTransactions = new HashMap<>();
        this.id_counter = 0;
        this.rnd = new Random();
        this.crashed = false;
        this.joinKeyCount = 0;
        this.is_joining = false;
        this.ongoing_set_keys = new HashSet<>();
        this.join_failed = false;
        this.stagedStorage = new HashMap<>();
    }

    static public Props props(int id) {
        return Props.create(Node.class, () -> new Node(id));
    }

    /// SET(k, v)

    /**
     * Set.InitiateMsg handler; finds responsibles for the data item, sets a timeout and requests the version to
     * the responsibles.
     *
     * @param msg Set.InitiateMsg message
     */
    private void receiveSet(Set.InitiateMsg msg) {
        if (this.crashed) {
            // debug
            System.out.println("!!! IGNORING SET ON CRASHED");
            return;
        }

        List<Peer> responsibles = this.getResponsibles(msg.key);

        int old_counter = this.id_counter++;

        this.setTransactions.put(old_counter, new SetTransaction(msg.key, msg.value, getSender()));
        Set.VersionRequestMsg reqMsg = new Set.VersionRequestMsg(msg.key, old_counter);

        setTimeout(new Set.TimeoutMsg(old_counter));

        for (Peer peer: responsibles) {
            sendMessageDelay(peer.ref, reqMsg);
        }
    }

    /**
     * Set.VersionRequestMsg handler; if the node already contains the data item, it returns its version,
     * otherwise it returns -1; it sends the result to the sender.
     *
     * @param msg Set.VersionRequestMsg message
     */
    private void receiveVersionRequest(Set.VersionRequestMsg msg) {
        if (this.crashed) return;

        if (!this.ongoing_set_keys.add(msg.key)) {
            return;
        }

        Entry entry = this.storage.get(msg.key);
        int version = entry==null?-1:entry.version;
        sendMessageDelay(getSender(), new Set.VersionResponseMsg(version, msg.transacition_id));
    }

    /**
     * Set.VersionResponseMsg handler; checks the quorum, tells the client about the success and updates the version of
     * the data item of all replicas.
     *
     * @param msg Set.VersionResponseMsg message
     */
    private void receiveVersionResponse(Set.VersionResponseMsg msg) {
        if (this.crashed) return;
        if (!this.setTransactions.containsKey(msg.transacition_id)) { return; }

        SetTransaction transaction = this.setTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.version);

        if (transaction.replies.size() < App.W) { return; }
        this.setTransactions.remove(msg.transacition_id);

        // FIXME: need to send the message with a delay?
        // transaction.client.tell(new Set.SuccessMsg(transaction.key), getSelf());
        sendMessageDelay(transaction.client, new Set.SuccessMsg(transaction.key));

        int maxVersion = 0;
        for (int response: transaction.replies) {
            if (response > maxVersion) {
                maxVersion = response;
            }
        }
        maxVersion++;

        List<Peer> responsibles = this.getResponsibles(transaction.key);

        Set.UpdateEntryMsg updateMsg = new Set.UpdateEntryMsg(transaction.key, new Entry(transaction.value, maxVersion));

        for (Peer responsible: responsibles) {
            sendMessageDelay(responsible.ref, updateMsg);
        }

        // debug
        System.out.println("SET "+transaction.key+" "+transaction.value+" "+maxVersion+" ");
    }

    /**
     * Set.UpdateEntryMsg handler; it inserts the updated entry in the local storage.
     *
     * @param msg Set.UpdateEntry message
     */
    private void receiveUpdateMessage(Set.UpdateEntryMsg msg) {
        if (this.crashed) return;
        this.storage.put(msg.key, msg.entry);
        this.ongoing_set_keys.remove(msg.key);

        // debug
        System.out.println("ADD "+this.id+" "+msg.key+" "+ msg.entry.value+" "+msg.entry.version);
    }

    /**
     * Set.TimeoutMsg handler; it removes the transaction, it sends a FailMsg to the client, and it unlocks the data
     * item key.
     *
     * @param msg Set.TimeoutMsg message
     */
    private void receiveSetTimeout(Set.TimeoutMsg msg) {
        if (this.crashed) return;
        SetTransaction transaction = this.setTransactions.remove(msg.transaction_id);

        if (transaction!=null) {
            //transaction.client.tell(new Set.FailMsg(transaction.key), getSelf());
            sendMessageDelay(transaction.client, new Set.FailMsg(transaction.key));
            for (Peer p: this.getResponsibles(transaction.key)) {
                sendMessageDelay(p.ref, new Set.UnlockMsg(transaction.key));
            }
        }
    }

    /**
     * Set.UnlockMsg handler; it removes the data item key from the ongoing set keys to be able to start new
     * set transactions on that data item.
     *
     * @param msg Set.UnlockMsg message
     */
    private void receiveSetUnlock(Set.UnlockMsg msg) {
        this.ongoing_set_keys.remove(msg.key);
    }

    /// GET(k)

    /**
     * Get.InitiateMsg handler; sets a timeout and sends a get request to all the responsible for the data item.
     *
     * @param msg Get.InitiateMsg message
     */
    public void receiveGet(Get.InitiateMsg msg) {
        if (this.crashed) {
            // debug
            System.out.println("!!! IGNORING GET ON CRASHED " + this.id + " " + msg.key);
            return;
        }
        List<Peer> responsibles = this.getResponsibles(msg.key);

        int old_counter = this.id_counter++;
        this.getTransactions.put(old_counter, new GetTransaction(msg.key, getSender()));
        Get.EntryRequestMsg reqMsg = new Get.EntryRequestMsg(msg.key, old_counter);

        setTimeout(new Get.TimeoutMsg(old_counter));

        for (Peer peer: responsibles) {
            sendMessageDelay(peer.ref, reqMsg);
        }
    }

    /**
     * Get.EntryRequestMsg handler; takes the entry from the local storage and sends it to the coordinator (the node
     * who received the get request).
     *
     * @param msg Get.EntryRequestMsg message
     */
    public void receiveEntryRequest(Get.EntryRequestMsg msg) {
        if (this.crashed) return;
        // if there is a concurrent set operation, the node stops performing the read operation
        if (this.ongoing_set_keys.contains(msg.key)) return;

        Entry entry = this.storage.get(msg.key);

        sendMessageDelay(getSender(), new Get.EntryResponseMsg(entry, msg.transacition_id));
    }

    /**
     *  Get.EntryResponseMsg handler; checks the quorum and sends the most updated data item to the client
     *  with a Get.SuccessMsg.
     *
     * @param msg Get.EntryResponseMsg message
     */
    public void receiveEntryResponse(Get.EntryResponseMsg msg) {
        if (this.crashed) return;
        if (!this.getTransactions.containsKey(msg.transacition_id)) { return; }
        GetTransaction transaction = this.getTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.entry);

        if (transaction.replies.size() < App.R) { return; }
        this.getTransactions.remove(msg.transacition_id);
        // find the most updated data item
        Entry latestEntry = transaction.replies.stream().filter(Objects::nonNull).max(Comparator.comparingInt(e->e.version)).orElse(null);

        if (latestEntry!=null){
            sendMessageDelay(transaction.client, new Get.SuccessMsg(transaction.key, latestEntry.value, latestEntry.version));

            // debug
            System.out.println("GET "+transaction.key+" "+latestEntry.value+" "+latestEntry.version+" ");
            // debug (sequential consistency)
            System.out.println("READ "+transaction.client.toString()+ " " +this.id+" "+transaction.key+" "+latestEntry.value + " " + latestEntry.version);
        }else{
            sendMessageDelay(transaction.client, new Get.FailMsg(transaction.key));
            //transaction.client.tell(new Get.FailMsg(transaction.key), getSelf());
        }
    }

    /**
     * Get.TimeoutMsg handler; removes the transaction from the pending ones and sends to the client a Get.FailMsg.
     *
     * @param msg Get.TimeoutMsg message
     */
    public void receiveGetTimeout(Get.TimeoutMsg msg) {
        if (this.crashed) return;
        GetTransaction transaction = this.getTransactions.remove(msg.transaction_id);

        if (transaction!=null) {
            sendMessageDelay(transaction.client, new Get.FailMsg(transaction.key));
        }
    }

    // JOIN

    /**
     * Join.InitiateMsg handler; the joining node contacts the bootstrapping peer to obtain the network topology.
     *
     * @param msg Join.InitiateMsg message
     */
    private void receiveJoinInitiate(Join.InitiateMsg msg){
        if (this.crashed) {return;}

        sendMessageDelay(msg.bootstrapping_peer, new Join.TopologyRequestMsg());
    }

    /**
     * Join.TopologyRequestMsg handler; the bootstrapping peer sends the topology to the joining node.
     *
     * @param msg Join.TopologyRequestMsg message
     */
    private void receiveTopologyRequest (Join.TopologyRequestMsg msg){
        if (this.crashed) {return;}

        sendMessageDelay(getSender(), new Join.TopologyResponseMsg(this.peers));
    }

    /**
     * Join.TopologyResponseMsg handler; the joining node asks its clockwise neighbor the data items it will be
     * responsible for.
     *
     * @param msg Join.TopologyResponseMsg message
     */
    private void receiveTopologyResponse (Join.TopologyResponseMsg msg){
        this.peers = msg.peers;
        this.is_joining = true;

        int myIndex = 0;
        while (myIndex < this.peers.size() && this.peers.get(myIndex).id < this.id) {myIndex++;}

        sendMessageDelay(this.peers.get(myIndex%this.peers.size()).ref, new Join.ResponsibilityRequestMsg(this.id));

        setTimeout(new Join.TimeoutMsg());
    }

    /**
     * Join.ResponsibilityRequestMsg handler; the clockwise neighbor of the joining node sends the data items the
     * joining node will be responsible for.
     *
     * @param msg Join.ResponsibilityRequestMsg message
     */
    // TODO: ask Diego about comment link in stream
    private void receiveResponsibilityRequest(Join.ResponsibilityRequestMsg msg){
        if (this.crashed) {return;}
        // it sends the data items that have a smaller key than the joining node id and the ones with bigger key
        // if we have less tha N nodes in the network
        sendMessageDelay(getSender(),new Join.ResponsibilityResponseMsg(this.storage.keySet().stream()
            // .filter(k -> k<msg.joining_id || peers.size()<N)
            .collect(Collectors.toSet())));
    }

    /**
     * Join.ResponsibilityResponseMsg handler; if the joining node receives one or more data items, it performs set
     * operations on them to obtain the most-updated value.
     *
     * @param msg Join.ResponsibilityResponseMsg message
     */
    private void receiveResponsibilityResponse(Join.ResponsibilityResponseMsg msg){
        if (!is_joining) {return;}
        this.is_joining = false;

        // if the joining node didn't receive data items, it announces itself
        if (msg.keys.isEmpty()){
            Stream.concat(
                this.peers.stream().map(p->p.ref),
                Stream.of(getSelf())
            ) .forEach(ref -> {
                    sendMessageDelay(ref, new Join.AnnouncePresenceMsg(this.id));
                });
            return;
        }

        this.joinKeyCount = msg.keys.size();
        this.join_failed = false;
        for (int k : msg.keys){
            sendMessageDelay(getSelf(), new Get.InitiateMsg(k));
        }
    }

    /**
     * Join.TimeoutMsg handler; if the joining node wasn't able to contact the bootstrapping peer, the join fails.
     *
     * @param msg Join.TimeoutMsg message
     */
    private void receiveJoinTimeout(Join.TimeoutMsg msg){
        if (!is_joining){return;}
        this.is_joining = false;

        // debug
        coordinator.tell(new Debug.FailMsg(Ops.JOIN, this.id, getSelf()), getSelf());
        System.out.println("JOIN_FAIL "+this.id);
    }

    /**
     * Get.SuccessMsg handler; the joining node inserts the data items to its storage and when all the read
     * operations are successful, it announces itself.
     *
     * @param msg Get.SuccessMsg message
     */
    private void receiveGetSuccess(Get.SuccessMsg msg){
        this.joinKeyCount--;

        // we are sure we received the max version because of quorum
        this.storage.put(msg.key, new Entry(msg.value, msg.version));

        // debug
        System.out.println("ADD "+this.id+" "+msg.key+" "+ msg.value+" "+msg.version);

        // when all the read operations completed, the joining node announces itself
        if (this.joinKeyCount==0){
            Stream.concat(
                this.peers.stream().map(p->p.ref),
                Stream.of(getSelf())
            ).forEach(ref -> {
                    sendMessageDelay(ref, new Join.AnnouncePresenceMsg(this.id));
                });
        }
    }

    /**
     * Get.FailMsg handler; the join operations fails when at least one read operation fails.
     *
     * @param msg Get.FailMsg message
     */
    private void receiveGetFail(Get.FailMsg msg){
        if (this.join_failed) return;
        this.join_failed = true;

        // debug
        coordinator.tell(new Debug.FailMsg(Ops.JOIN, this.id, getSelf()), getSelf());
        System.out.println("JOIN_FAIL "+this.id);
    }

    /**
     * Join.AnnouncePresenceMsg handler; the nodes insert the joining node in the network topology and remove the data
     * they are no longer responsible for.
     *
     * @param msg Join.AnnouncePresenceMsg message
     */
    private void receivePresenceAnnouncement(Join.AnnouncePresenceMsg msg) {
        if (this.crashed) return;
        // Add the new node to the list of peers
        int i=0;
        while (i<this.peers.size() && msg.id > this.peers.get(i).id) {
            i++;
        }
        this.peers.add(i, new Peer(msg.id, getSender()));

        // Remove the data you're no longer responsible for
        Iterator<Map.Entry<Integer, Entry>> it = this.storage.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Entry> entry = it.next();
            List<Peer> responsibles = this.getResponsibles(entry.getKey());
            if (responsibles.stream().noneMatch(p -> p.id == this.id)) {
                it.remove();

                // debug
                System.out.println("DELETE "+this.id+" "+entry.getKey());
            }
        }

        if (msg.id==this.id){
            // debug
            coordinator.tell(new Debug.SuccessMsg(Ops.JOIN, this.id, getSelf()), getSelf());
            System.out.println("JOIN "+this.id);
        }
    }

    // LEAVE

    /**
     * Leave.InitiateMsg handler; the leaving node says to other nodes that it is leaving the network of ot doesn't
     * have data items, otherwise it sends its data items to the new responsible for them.
     *
     * @param msg Leave.InitiateMsg message
     */
    private void receiveLeave(Leave.InitiateMsg msg) {
        if (this.crashed) return;

        int myIndex = 0;
        while (this.peers.get(myIndex).id!=this.id) myIndex++;
        this.peers.remove(myIndex);

        // Early exit; it the leaving node doesn't have data items it is responsible for it can simply leave the
        // network
        if (this.storage.isEmpty()) {
            AnnounceLeavingMsg leavingAnnouncement = new AnnounceLeavingMsg(false);
            for (Peer p: peers) {
                sendMessageDelay(p.ref, leavingAnnouncement);
            }

            // debug
            this.coordinator.tell(new Debug.SuccessMsg(Ops.LEAVE, this.id, getSelf()), getSelf());
            System.out.println("LEAVE "+this.id);
            return;
        }

        // Group data items with same future owner
        Map<Peer, List<Pair<Integer,Entry>>> buckets = new HashMap<>();
        for (HashMap.Entry<Integer,Entry> e: this.storage.entrySet()) {
            int key = e.getKey();
            Entry entry = e.getValue();
            List<Peer> responsibles = getResponsibles(key);
            Peer newResponsible = responsibles.get(responsibles.size()-1);

            if (!buckets.containsKey(newResponsible)) {
                buckets.put(newResponsible, new LinkedList<>());
            }
            buckets.get(newResponsible).add(new Pair<Integer, Entry>(key, entry));
        }

        this.is_leaving = true;
        this.leavingCount = buckets.size();

        setTimeout(new Leave.TimeoutMsg());
        for (HashMap.Entry<Peer, List<Pair<Integer,Entry>>> e: buckets.entrySet()) {
            sendMessageDelay(e.getKey().ref, new Leave.TransferItemsMsg(e.getValue()));
        }
    }

    /**
     * Leave.TransferItemsMsg handler; the new responsible nodes insert the leaving node data items to their staged
     * storage and inform the leaving node.
     *
     * @param msg Leave.TransferItemsMsg message.
     */
    private void receiveTransferItems(Leave.TransferItemsMsg msg) {
        if (this.crashed) return;
        this.stagedStorage.clear();
        for (Pair<Integer, Entry> dataItem: msg.items) {
            this.stagedStorage.put(dataItem.first(), dataItem.second());
            System.out.println("STAGE " + this.id + " " + dataItem.first() + " " + dataItem.second().value + " " + dataItem.second().version);
        }
        sendMessageDelay(getSender(), new Leave.AckMsg());
    }

    /**
     * Leave.AckMsg handler; if all the new responsible nodes received the data items, the leaving node announces
     * itself.
     *
     * @param msg Leave.AckMsg message
     */
    private void receiveAck(Leave.AckMsg msg) {
        this.leavingCount--;
        if (this.is_leaving && this.leavingCount==0) {
            this.is_leaving = false;

            AnnounceLeavingMsg leavingAnnouncement = new AnnounceLeavingMsg(true);
            for (Peer p: peers) {
                sendMessageDelay(p.ref, leavingAnnouncement);
            }

            // debug
            this.coordinator.tell(new Debug.SuccessMsg(Ops.LEAVE, this.id, getSelf()), getSelf());
            System.out.println("LEAVE "+this.id);
        }
    }

    /**
     * Leave.TimeoutMsg handler; if the leave operation fails (there aren't sufficient nodes that can store the
     * leaving node data items), we insert back the leaving node into the network.
     *
     * @param msg Leave.TimeoutMsg message
     */
    private void receiveLeavingTimeout(Leave.TimeoutMsg msg) {
        if (!this.is_leaving) return;
        this.is_leaving = false;

        // debug
        this.coordinator.tell(new Debug.FailMsg(Ops.LEAVE, this.id, getSelf()), getSelf());
        
        int myIndex = 0;
        while (myIndex<peers.size() && this.id > peers.get(myIndex).id) myIndex++;
        this.peers.add(myIndex, new Peer(this.id, getSelf()));

        // debug
        System.out.println("LEAVE_FAIL "+this.id);
    }

    /**
     * Leave.AnnounceLeavingMsg handler; the nodes remove the leaving node from the network and put the data items
     * of the staged storage in their storage.
     *
     * @param msg Leave.AnnounceLeavingMsg message
     */
    private void receiveAnnounceLeave(Leave.AnnounceLeavingMsg msg) {
        if (this.crashed) return;

        this.peers.removeIf(p->p.ref.equals(getSender()));
        if (msg.insert_staged_keys) {
            for (HashMap.Entry<Integer, Entry> stagedEntry: this.stagedStorage.entrySet()) {
                if (!isResponsible(stagedEntry.getKey(), getSelf())) continue;
                Entry old_entry = this.storage.get(stagedEntry.getKey());
                if (old_entry == null || old_entry.version<stagedEntry.getValue().version) {
                    this.storage.put(stagedEntry.getKey(), stagedEntry.getValue());

                    // debug
                    System.out.println("ADD "+this.id+" "+stagedEntry.getKey()+" "+ stagedEntry.getValue().value+" "+stagedEntry.getValue().version);
                }
            }
            stagedStorage.clear();
        }
    }

    /// CRASH

    /**
     * Crash.InitiateMsg handler; the node crashes setting its crashed attribute to true.
     *
     * @param msg Crash.InitiateMsg message
     */
    private void receiveCrash(Crash.InitiateMsg msg) {
        if (this.crashed) return;
        this.crashed = true;

        // debug
        System.out.println("CRASH "+this.id);
        coordinator.tell(new Debug.SuccessMsg(Ops.CRASH, this.id, getSelf()),getSelf());
    }

    /// RECOVERY

    /**
     * Crash.RecoveryMsg handler; the node recovers setting its crashed attribute to false and send a message to the
     * helper requesting the network topology.
     *
     * @param msg Crash.RecoveryMsg message
     */
    private void receiveRecovery(Crash.RecoveryMsg msg) {
        this.crashed=false;

        sendMessageDelay(msg.helper, new Crash.TopologyRequestMsg());
    }

    /**
     * Crash.TopologyRequestMsg handler; it sends the network topology to the recovered node.
     *
     * @param msg Crash.TopologyRequestMsg message
     */
    private void receiveTopologyRequest(Crash.TopologyRequestMsg msg) {
        if (this.crashed) return;

        sendMessageDelay(getSender(), new Crash.TopologyResponseMsg(this.peers));
    }

    /**
     * Crash.TopologyResponseMsg handler; it deletes elements that are no more under the recovered node control
     * and ask for elements to the nodes that own the recovered node data items.
     *
     * @param msg Crash.TopologyResponseMsg message
     */
    private void receiveTopologyResponse(Crash.TopologyResponseMsg msg) {
        if (this.crashed) return;
        this.peers = msg.peers;

        // Delete elements the recovered node will no more be responsible for
        Iterator<HashMap.Entry<Integer,Entry>> it = this.storage.entrySet().iterator();
        while (it.hasNext()) {
            HashMap.Entry<Integer,Entry> entry = it.next();
            List<Peer> peers = this.getResponsibles(entry.getKey());
            if (!peers.stream().filter(p -> p.id == this.id).findFirst().isPresent()) {
                it.remove();

                // debug
                System.out.println("DELETE "+this.id+" "+entry.getKey());
            }
        }

        // Ask for elements: only contact the N-1 nodes behind and the N-1 nodes ahead of yourself,
        // they're the only ones with data you may be interested in
        int myIndex = 0;
        while (this.peers.get(myIndex).id != this.id) {myIndex++;}

        Crash.RequestDataMsg requestDataMsg = new Crash.RequestDataMsg(this.id);
        for (int i = -N+1; i< N; i++) {
            if (i==0) continue;
            int j = (i+myIndex+this.peers.size())%this.peers.size();

            sendMessageDelay(this.peers.get(j).ref, requestDataMsg);
        }

        // debug
        coordinator.tell(new Debug.SuccessMsg(Ops.RECOVER, this.id, getSelf()), getSelf());
        System.out.println("RECOVERY "+this.id);
    }

    /**
     * Crash.RequestDataMsg handler; it sends the data the recovered node is responsible for.
     *
     * @param msg Crash.RequestDataMsg message
     */
    private void receiveDataRequest(Crash.RequestDataMsg msg) {
        if (this.crashed) return;
        // find elements the recovered node is responsible for
        List<Pair<Integer, Entry>> data = new LinkedList<>();
        for (HashMap.Entry<Integer, Entry> entry: this.storage.entrySet()) {
            if (isResponsible(entry.getKey(), getSender())) {
                data.add(new Pair<>(entry.getKey(), entry.getValue()));
            }
        }

        sendMessageDelay(getSender(), new Crash.DataResponseMsg(data));
    }

    /**
     * Crash.DataResponseMsg handler; inserts/updates the data items the recovered node is responsible for.
     *
     * @param msg Crash.DataResponseMsg message
     */
    private void receiveDataResponse(Crash.DataResponseMsg msg) {
        if (this.crashed) return;
        for (Pair<Integer, Entry> dataItem: msg.data) {
            if (!this.storage.containsKey(dataItem.first()) || this.storage.get(dataItem.first()).version < dataItem.second().version) {
                this.storage.put(dataItem.first(), dataItem.second());

                // debug
                System.out.println("ADD "+this.id+" "+dataItem.first()+" "+dataItem.second().value+" "+dataItem.second().version); // DEBUG
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
        .match(Set.TimeoutMsg.class,this::receiveSetTimeout)
        .match(Set.UnlockMsg.class,this::receiveSetUnlock)
        .match(Debug.AddNodeMsg.class, this::receiveDebugAddNode)
        .match(Get.InitiateMsg.class, this::receiveGet)
        .match(Get.EntryRequestMsg.class, this::receiveEntryRequest)
        .match(Get.EntryResponseMsg.class, this::receiveEntryResponse)
        .match(Get.TimeoutMsg.class, this::receiveGetTimeout)
        .match(Join.InitiateMsg.class, this::receiveJoinInitiate)
        .match(Join.TopologyRequestMsg.class, this::receiveTopologyRequest)
        .match(Join.TopologyResponseMsg.class, this::receiveTopologyResponse)
        .match(Join.TimeoutMsg.class, this::receiveJoinTimeout)
        .match(Get.SuccessMsg.class, this::receiveGetSuccess)
        .match(Get.FailMsg.class, this::receiveGetFail)
        .match(Join.ResponsibilityRequestMsg.class, this::receiveResponsibilityRequest)
        .match(Join.ResponsibilityResponseMsg.class, this::receiveResponsibilityResponse)
        .match(Join.AnnouncePresenceMsg.class, this::receivePresenceAnnouncement)
        .match(Leave.InitiateMsg.class, this::receiveLeave)
        .match(Leave.AnnounceLeavingMsg.class, this::receiveAnnounceLeave)
        .match(Leave.TransferItemsMsg.class, this::receiveTransferItems)
        .match(Leave.AckMsg.class, this::receiveAck)
        .match(Leave.TimeoutMsg.class, this::receiveLeavingTimeout)
        .match(Debug.AnnounceCoordinator.class, this::receiveAnnounceCoordinator)
        .match(Crash.InitiateMsg.class, this::receiveCrash)
        .match(Crash.RecoveryMsg.class, this::receiveRecovery)
        .match(Crash.TopologyRequestMsg.class, this::receiveTopologyRequest)
        .match(Crash.TopologyResponseMsg.class, this::receiveTopologyResponse)
        .match(Crash.RequestDataMsg.class, this::receiveDataRequest)
        .match(Crash.DataResponseMsg.class, this::receiveDataResponse)
        .build();
    }
}

/* 
 * TODO
 * 1. USE `amIResponsible` method that tells if I'm responsible for a data item
 * 2. CHECK all crash condition, sometimes they are not needed for project assumptions (maybe we insert them in any case?)
 */
