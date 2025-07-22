package org.example;

import static org.example.App.MSG_MAX_DELAY;

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
import scala.concurrent.duration.Duration;

import org.example.msg.*;
import org.example.msg.Leave.AnnounceLeavingMsg;
import org.example.msg.Leave.TransferItemsMsg;

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
    private Random rnd;
    private int joiningQuorum;

    private ActorRef coordinator;

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
    /**
     * Overload for {@link #sendMessageDelay(ActorRef, Serializable, ActorRef, int)} with default duration
     */
    private void sendMessageDelay(ActorRef target, Serializable msg, ActorRef sender) {
        sendMessageDelay(target, msg, sender, MSG_MAX_DELAY);
    }
    /**
     * Overload for {@link #sendMessageDelay(ActorRef, Serializable, ActorRef, int)} with default duration and sender
     */
    private void sendMessageDelay(ActorRef target, Serializable msg) {
        sendMessageDelay(target, msg, getSelf(), MSG_MAX_DELAY);
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
        return (!peers.stream().filter(p -> p.ref == node).findFirst().isPresent());
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
        for (int j=0; j<App.N; j++) {
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

    /**
     * The class represents an entry of the local storage.
     */
    public class Entry {
        /** String name of the data item. */
        public String value;
        public int version;
        public Entry (String value, int version) {
            this.value = value;
            this.version = version;
        }
    }

    /**
     * The class represents a peer in the network.
     */
    public static class Peer {
        /** ID of the peer. */
        public int id;
        /** Actor reference of the peer. */
        public ActorRef ref;
        public Peer (int id, ActorRef ref) {
            this.id = id;
            this.ref = ref;
        }

        // debug
        @Override
        public String toString() {
            return "Peer{" +
            "id=" + id +
            '}';
        }
    }

    /**
     * The class represents a set request (adding or updating a data item).
     */
    private class SetTransaction {
        /** Key of data item. */
        public final int key;
        /** Value of the data item. */
        public final String value;
        /** List of versions of the collected replies. */
        public final List<Integer> replies;
        /** Actor reference of the client that made the set request. */
        public final ActorRef client;

        /**
         * Constructor of class SetTransaction.
         *
         * @param key key of the data item
         * @param value value of the data item
         * @param client client that made the request
         */
        public SetTransaction(int key, String value, ActorRef client) {
            this.key = key;
            this.value = value;
            this.replies = new LinkedList<>();
            this.client = client;
        }
    }

    /**
     * The class represents a get request (read a data item).
     */
    private class GetTransaction {
        /** Key of the data item. */
        public final int key;
        /** List of entries of the collected replies. */
        public final List<Entry> replies;
        /** Actor reference of the client that made the get request. */
        public final ActorRef client;

        /**
         * Constructor of class GetTransaction.
         *
         * @param key key of the data item
         * @param client Actor reference of the client that made the request
         */
        public GetTransaction(int key, ActorRef client) {
            this.key = key;
            this.replies = new LinkedList<>();
            this.client = client;
        }
    }

    /// CONSTRUCTOR

    /**
     * Class Node constructor
     *
     * @param id ID of the node
     */
    public Node (int id) {
        this.id = id;
        this.storage = new HashMap<>();
        this.peers = new ArrayList<>();
        this.setTransactions = new HashMap<>();
        this.getTransactions = new HashMap<>();
        this.id_counter = 0;
        this.rnd = new Random();
        this.crashed = false;
        this.joiningQuorum = 0;
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
        if (this.crashed) return;
        List<Peer> responsibles = this.getResponsibles(msg.key);

        // VersionRequest message creation
        this.setTransactions.put(this.id_counter, new SetTransaction(msg.key, msg.value, getSender()));
        Set.VersionRequestMsg reqMsg = new Set.VersionRequestMsg(msg.key, this.id_counter);

        // TimeoutMsg creation and send
        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.MILLISECONDS),
            getSelf(),
            new Set.TimeoutMsg(this.id_counter), getContext().system().dispatcher(),
            getSelf()
        );

        this.id_counter++;

        // VersionRequestMsg send
        for (Peer peer: responsibles) {
            sendMessageDelay(peer.ref, reqMsg);
        }
    }

    /**
     * Set.VersionRequestMsg handler; if the node already contains the data item, it returns its version,
     *  otherwise it returns -1; it sends the result to the sender.
     *
     * @param msg Set.VersionRequestMsg message
     */
    private void receiveVersionRequest(Set.VersionRequestMsg msg) {
        if (this.crashed) return;
        Entry entry = this.storage.get(msg.key);
        int version = entry==null?-1:entry.version;
        // VersionResponse message creation and send
        getSender().tell(new Set.VersionResponseMsg(version, msg.transacition_id), getSelf());
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
        // select the right transaction
        SetTransaction transaction = this.setTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.version);
        // check the quorum
        if (transaction.replies.size() < App.W) { return; }
        this.setTransactions.remove(msg.transacition_id);

        // Success message creation and send
        transaction.client.tell(new Set.SuccessMsg(), getSelf());

        // find the max version and increase it by one
        int maxVersion = 0;
        for (int response: transaction.replies) {
            if (response > maxVersion) {
                maxVersion = response;
            }
        }
        maxVersion++;

        List<Peer> responsibles = this.getResponsibles(transaction.key);

        // UpdateEntry message creation (send the data item with the updated version to all responsibles)
        Set.UpdateEntryMsg updateMsg = new Set.UpdateEntryMsg(transaction.key, new Entry(transaction.value, maxVersion));
        // UpdateEntry message send
        for (Peer responsible: responsibles) {
            sendMessageDelay(responsible.ref, updateMsg);
        }
    }

    /**
     * Set.UpdateEntryMsg handler; it inserts the updated entry in the local storage.
     *
     * @param msg Set.UpdateEntry message
     */
    private void receiveUpdateMessage(Set.UpdateEntryMsg msg) {
        if (this.crashed) return;
        this.storage.put(msg.key, msg.entry);
        System.out.println("W "+this.id+" "+msg.key+" "+msg.entry.version);
    }

    /**
     * Set.TimeoutMsg handler; removes the transaction and sends a FailMsg to the client.
     *
     * @param msg Set.TimeoutMsg message
     */
    private void receiveSetTimeout(Set.TimeoutMsg msg) {
        if (this.crashed) return;
        SetTransaction transaction = this.setTransactions.remove(msg.transaction_id);
        // Set.Fail message creation and send
        if (transaction!=null) {
            transaction.client.tell(new Set.FailMsg(), getSelf());

            // debug
            coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());
        }
    }

    /// GET(k)

    /**
     * Get.InitiateMsg handler; sets a timeout and sends a get request to all the responsibles for the data item.
     *
     * @param msg Get.InitiateMsg message
     */
    public void receiveGet(Get.InitiateMsg msg) {
        if (this.crashed) return;
        List<Peer> responsibles = this.getResponsibles(msg.key);

        this.getTransactions.put(this.id_counter, new GetTransaction(msg.key, getSender()));
        // EntryRequest message creation
        Get.EntryRequestMsg reqMsg = new Get.EntryRequestMsg(msg.key, this.id_counter);

        // Timeout message creation and send
        getContext().system().scheduler().scheduleOnce(
            Duration.create(App.T, TimeUnit.MILLISECONDS),
            getSelf(),
            new Get.TimeoutMsg(this.id_counter), getContext().system().dispatcher(),
            getSelf()
        );
        this.id_counter++;

        // EntryRequest message send
        for (Peer peer: responsibles) {
            sendMessageDelay(peer.ref, reqMsg);
        }
    }

    /**
     * Get.EntryRequestMsg handler; takes the entry from the local storage and sends it to the coordinator.
     *
     * @param msg Get.EntryRequestMsg message
     */
    public void receiveEntryRequest(Get.EntryRequestMsg msg) {
        if (this.crashed) return;
        Entry entry = this.storage.get(msg.key);
        // EntryResponse message creation and send
        sendMessageDelay(getSender(), new Get.EntryResponseMsg(entry, msg.transacition_id));
    }

    /**
     *  Get.EntryResponseMsg handler; checks the quorum ands sends the most updated data item to the client
     *  with a Get.SuccessMsg.
     *
     * @param msg Get.EntryResponseMsg message
     */
    public void receiveEntryResponse(Get.EntryResponseMsg msg) {
        if (this.crashed) return;
        if (!this.getTransactions.containsKey(msg.transacition_id)) { return; }
        GetTransaction transaction = this.getTransactions.get(msg.transacition_id);
        transaction.replies.add(msg.entry);
        // quorum check
        if (transaction.replies.size() < App.R) { return; }
        this.getTransactions.remove(msg.transacition_id);
        // find the most updated data item
        Entry latestEntry = null;
        for (Entry entry: transaction.replies) {
            if (entry!=null && (latestEntry==null || entry.version > latestEntry.version)) {
                latestEntry = entry;
            }
        }
        // Success message creation and send
        if (latestEntry!=null){
            transaction.client.tell(new Get.SuccessMsg(transaction.key, latestEntry.value), getSelf());
            coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());

            // debug
            coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());
            System.out.println("R "+transaction.client.toString()+ " " +this.id+" "+transaction.key+" "+latestEntry.value);
        }else{
            transaction.client.tell(new Get.FailMsg(transaction.key), getSelf());
            coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());
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
        // Get.Fail message creation and send
        if (transaction!=null) {
            transaction.client.tell(new Get.FailMsg(transaction.key), getSelf());
            coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());
        }
    }

    // JOIN

    /**
     * Join.InitiateMsg handler; sends the network topology to the joining node.
     *
     * @param msg Join.InitiateMsg message
     */
    private void receiveJoinInitiate(Join.InitiateMsg msg) {
        if (this.crashed) return;
        // Join.TopologyMsg creation and send
        sendMessageDelay(getSender(), new Join.TopologyMsg(this.peers));
    }
    /**
     * Join.TopologyMsg handler; the joining node receives the network topology and sends a message to get the data
     * it is responsible for.
     *
     * @param msg Join.TopologyMsg message
     */
    private void receiveTopology(Join.TopologyMsg msg) {
        if (this.crashed) return;
        this.peers.addAll(msg.peers);
        List<Peer> neighbors = this.getResponsibles(this.id);
        for (Peer neighbor: neighbors) {
            sendMessageDelay(neighbor.ref, new Join.ResponsibilityRequestMsg(this.id));
        }
    }
    /**
     * Join.ResponsibilityRequestMsg handler; it finds the data items the joining node is responsible for and sends them
     * to it.
     *
     * @param msg Join.ResponsibilityRequestMsg message
     */
    private void receiveResponsibilityRequest(Join.ResponsibilityRequestMsg msg) {
        if (this.crashed) return;
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
        if (newId > peers.get(peers.size()-1).id) {
            allNodes.add(new Peer(newId, newRef));
        }

        // Find the data the joining node is responsible for
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

        // Join.ResponsibilityResponseMsg creation and send
        sendMessageDelay(getSender(), new Join.ResponsibilityResponseMsg(ret));
    }
    /**
     * Join.ResponsibilityResponseMsg handler; it checks the quorum, inserts the most-up-to-date data items in the
     * joining node local storage and announces the presence of the new node to the others.
     * @param msg
     */
    private void receiveResponsibilityResponse(Join.ResponsibilityResponseMsg msg) {
        if (this.crashed) return;
        // check quorum
        if (joiningQuorum >= App.R) {return;}
        // insert most-up-to-date data items
        for (HashMap.Entry<Integer, Entry> entry: msg.responsibility.entrySet()) {
            Entry currentValue = storage.get(entry.getKey());
            if (currentValue == null || currentValue.version < entry.getValue().version) {
                this.storage.put(entry.getKey(), entry.getValue());
                System.out.println("A "+this.id+" "+entry.getKey()+" "+entry.getValue().version);
            }
        }
        joiningQuorum++;

        // we reach the quorum
        if (joiningQuorum>=App.R) {
            Stream.concat(
                this.peers.stream(),
                Stream.of(new Peer(this.id, getSelf()))
            ).forEach(peer -> {
                    // Join.AnnouncePresenceMsg creation and send
                    sendMessageDelay(peer.ref, new Join.AnnouncePresenceMsg(this.id));
                });
        }
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
            if (!responsibles.stream().anyMatch(p -> p.id == this.id)) {
                it.remove();
                System.out.println("D "+this.id+" "+entry.getKey());
            }
        }

        // debug

        coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());

        if (msg.id==this.id){
            System.out.println("J "+this.id);
        }

    }

    // LEAVE

    private void receiveLeave(Leave.InitiateMsg msg) {
        if (this.crashed) return;
        AnnounceLeavingMsg announcementMsg = new AnnounceLeavingMsg();
        for (Peer peer: this.peers) {
            sendMessageDelay(peer.ref, announcementMsg);
        }
    }

    private void receiveAnnounceLeave(Leave.AnnounceLeavingMsg msg) {
        if (this.crashed) return;
        this.peers = this.peers.stream().filter(p -> p.ref!=getSender()).collect(Collectors.toList());
        if (getSelf() != getSender()) return;

        HashMap<Peer, LinkedList<Pair<Integer, Entry>>> buckets = new HashMap<>();

        // Decide which elements to send to each peer
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
            sendMessageDelay(bucket.getKey().ref, new TransferItemsMsg(bucket.getValue()));
            coordinator.tell(new Debug.IncreaseOngoingMsg(bucket.getKey().ref),ActorRef.noSender());
        }
    }

    private void receiveTransferItems(TransferItemsMsg msg) {
        if (this.crashed) return;
        for (Pair<Integer, Entry> dataItem: msg.items) {
            if (!this.storage.containsKey(dataItem.first()) || this.storage.get(dataItem.first()).version < dataItem.second().version) {
                this.storage.put(dataItem.first(), dataItem.second());
            }
        }
        coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());

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
        coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());
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
        // Crash.TopologyRequestMsg creation and send
        sendMessageDelay(msg.helper, new Crash.TopologyRequestMsg());
    }
    /**
     * Crash.TopologyRequestMsg handler; it sends the network topology to the recovered node.
     *
     * @param msg Crash.TopologyRequestMsg message
     */
    private void receiveTopologyRequest(Crash.TopologyRequestMsg msg) {
        if (this.crashed) return;
        // Crash.TopologyResponseMsg creation and send
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
        for (HashMap.Entry<Integer, Entry> entry: this.storage.entrySet()) {
            List<Peer> peers = this.getResponsibles(entry.getKey());
            if (!peers.stream().filter(p -> p.id == this.id).findFirst().isPresent()) {
                this.storage.remove(entry.getKey());
            }
        }

        // Ask for elements: only contact the N-1 nodes behind and the N-1 nodes ahead of yourself,
        // they're the only ones with data you may be interested in
        int myIndex = 0;
        // Crash.RequestDataMsg creation
        Crash.RequestDataMsg requestDataMsg = new Crash.RequestDataMsg(this.id);
        while (this.peers.get(myIndex).id != this.id) {myIndex++;}
        for (int i=-App.N+1; i<App.N; i++) {
            if (i==0) continue;
            int j = (i+myIndex+this.peers.size())%this.peers.size();
            // Crash.RequestDataMsg send
            sendMessageDelay(this.peers.get(j).ref, requestDataMsg);
            coordinator.tell(new Debug.IncreaseOngoingMsg(this.peers.get(j).ref),ActorRef.noSender());
        }
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
        // Crash.DataResponseMsg creation and send
        sendMessageDelay(getSender(), new Crash.DataResponseMsg(data));
    }

    /**
     * Crash.DataResponseMsg handler; inserts/updates the data items the recovered node is responsible for.
     *
     * @param msg Crash.DataResponseMsg message
     */
    private void receiverDataResponse(Crash.DataResponseMsg msg) {
        if (this.crashed) return;
        for (Pair<Integer, Entry> dataItem: msg.data) {
            if (!this.storage.containsKey(dataItem.first()) || this.storage.get(dataItem.first()).version < dataItem.second().version) {
                this.storage.put(dataItem.first(), dataItem.second());
            }
        }
        coordinator.tell(new Debug.DecreaseOngoingMsg(),getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Set.InitiateMsg.class, this::receiveSet)
        .match(Set.VersionRequestMsg.class, this::receiveVersionRequest)
        .match(Set.VersionResponseMsg.class, this::receiveVersionResponse)
        .match(Set.UpdateEntryMsg.class, this::receiveUpdateMessage)
        .match(Set.TimeoutMsg.class,this::receiveSetTimeout)
        .match(Debug.AddNodeMsg.class, this::receiveDebugAddNode)
        .match(Get.InitiateMsg.class, this::receiveGet)
        .match(Get.EntryRequestMsg.class, this::receiveEntryRequest)
        .match(Get.EntryResponseMsg.class, this::receiveEntryResponse)
        .match(Get.TimeoutMsg.class, this::receiveGetTimeout)
        .match(Join.InitiateMsg.class, this::receiveJoinInitiate)
        .match(Join.TopologyMsg.class, this::receiveTopology)
        .match(Join.ResponsibilityRequestMsg.class, this::receiveResponsibilityRequest)
        .match(Join.ResponsibilityResponseMsg.class, this::receiveResponsibilityResponse)
        .match(Join.AnnouncePresenceMsg.class, this::receivePresenceAnnouncement)
        .match(Leave.InitiateMsg.class, this::receiveLeave)
        .match(Leave.AnnounceLeavingMsg.class, this::receiveAnnounceLeave)
        .match(Leave.TransferItemsMsg.class, this::receiveTransferItems)
        .match(Debug.AnnounceCoordinator.class, this::receiveAnnounceCoordinator)
        .build();
    }
}

/**
 * Assumptions and Considerations
 * The system will start with N nodes.
 * If this were not the case, the quorum during the joining
 * operation for the first nodes wouldn't be met
 */

/* 
 * TODO
 * 1. USE `amIResponsible` method that tells if I'm responsible for a data item
 * 2. CHECK all crash condition, sometimes they are not needed for project assumptions
 */
