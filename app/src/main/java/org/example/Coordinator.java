package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import org.example.Node.Peer;
import org.example.msg.*;
import org.example.msg.Set;
import org.example.shared.NameGenerator;
import org.example.shared.NamedClient;
import org.example.shared.RngList;

import java.util.*;

import static org.example.App.*;

/**
 * The class represents a coordinator that performs a simulation providing project assumptions and requirements.
 */
public class Coordinator extends AbstractActor {

    /// ATTRIBUTES

    /** Number of keys */
    private final static int K = 4;

    /** List of clients */
    private RngList<NamedClient> clients;
    /** List of nodes in the system */
    private RngList<Peer> nodes_in;
    /** List of nodes out from the system */
    private RngList<Peer> nodes_out;
    /** List of crashed nodes */
    private RngList<Peer> crashed_nodes;
    /** List of all nodes (in and out) */
    private List<Peer> all_nodes;

    /** Maximum ID of the nodes (in and out) */
    private int max_id;
    /** Array of keys */
    private int [] keys;

    /** Counter of the actions that need to finish before starting another round */
    private int ongoing_actions;
    /** Number of current round */
    private int current_round;

    private Random rng;

    /// CONSTRUCTOR

    /**
     * Constructor of the Coordinator class.
     */
    public Coordinator() {
        this.rng = new Random();
        this.clients = new RngList<>(rng);
        this.nodes_in = new RngList<>(rng);
        this.nodes_out = new RngList<>(rng);
        this.crashed_nodes = new RngList<>(rng);
        this.max_id = 0;
        this.keys = new int[K];
        this.ongoing_actions = 0;
        this.current_round = 0;
        this.all_nodes = new ArrayList<>();
    }

    static public Props props() {
        return Props.create(Coordinator.class, () -> new Coordinator());
    }

    /// METHODS

    /**
     * Handler of Debug.AddClientMsg; it adds the client to the list of clients.
     *
     * @param msg Debug.AddClientMsg message
     */
    private void receiveAddClientMsg(Debug.AddClientMsg msg){
        this.clients.add(new NamedClient(msg.name, msg.ref));
    }

    /**
     * Debug.AddNodesMsg handler; it adds the nodes in/out and updates keys.
     *
     * @param msg Debug.AddNodesMsg message
     */
    private void receiveAddNodesMsg(Debug.AddNodesMsg msg){
        this.nodes_in.addAll(msg.peers_in);
        this.nodes_out.addAll(msg.peers_out);

        all_nodes.addAll(this.nodes_in);
        all_nodes.addAll(this.nodes_out);
        updateKeys(all_nodes);
    }

    /**
     * Debug.StartRoundMsg handler; it chooses between performing a set/get round or a join/leave/crash/recovery round
     * and executes it.
     *
     * @param msg Debug.StartRoundMsg message
     */
    private void receiveStartRoundMsg(Debug.StartRoundMsg msg){
        current_round++;
        if (current_round>=ROUNDS){
            System.err.println("> PRESS ENTER <");
            return;
        }

        System.out.println("///// STARTING ROUND "+current_round);
        System.err.println("///// STARTING ROUND "+current_round);

        ongoing_actions = 0;

        // READ and WRITE
        if (rng.nextBoolean()){
            ongoing_actions = clients.size();
            // each client performs a read or a write operation
            for (NamedClient client : clients){
                Peer node;
                do {
                    node = nodes_in.getRandom();
                    System.err.println("Retry");
                }while(crashed_nodes.contains(node));

                int key = keys[rng.nextInt(K)];
                if (rng.nextBoolean()) {
                    // WRITE
                    String fruit = NameGenerator.getFruit();
                    System.out.println(("SET (" + key + ": " + fruit + ")"));
                    node.ref.tell(new Set.InitiateMsg(key, fruit), client.ref);
                } else {
                    // READ
                    node.ref.tell(new Get.InitiateMsg(key), client.ref);
                    System.out.println(("GET ("+ key +")"));
                }
            }
        }else{ // JOIN, LEAVE, CRASH, RECOVERY
            Peer node;
            do {
                node = nodes_in.getRandom();
                System.err.println("Retry");
            }while(crashed_nodes.contains(node));

            ongoing_actions = 1;
            switch (rng.nextInt(4)){
                // JOIN
                case 0:
                if (nodes_out.isEmpty()) {
                    getSelf().tell(new Debug.StartRoundMsg(), getSelf());
                    System.out.println("No nodes available to join");
                    return;
                }
                Peer new_node = nodes_out.getRandom();

                System.out.println("JOIN ("+new_node.id+")");
                new_node.ref.tell(new Join.InitiateMsg(node.ref), ActorRef.noSender());
                break;

                // LEAVE
                case 1:
                System.out.println("LEAVE (" + node.id +")");

                if (nodes_in.size() == N) {
                    System.out.println("Can't have less than N nodes");
                    getSelf().tell(new Debug.StartRoundMsg(), getSelf());
                    return;
                }
                node.ref.tell(new Leave.InitiateMsg(), ActorRef.noSender());
                break;

                // CRASH
                case 2:

                // condition to avoid total crash failure
                if (crashed_nodes.size()==nodes_in.size()-1) {
                    System.out.println("Can't crash only remaining node");
                    getSelf().tell(new Debug.StartRoundMsg(), getSelf());
                    return;
                }

                System.out.println("CRASH (" + node.id +")");
                node.ref.tell(new Crash.InitiateMsg(),ActorRef.noSender());
                break;

                // RECOVERY
                case 3:
                if (crashed_nodes.size()==0) {
                    System.out.println("No crashed nodes to recover");
                    getSelf().tell(new Debug.StartRoundMsg(),getSelf());
                    return;
                }

                Peer crashed_node = crashed_nodes.getRandom();
                System.out.println("RECOVERY (" + crashed_node.id +")");

                crashed_node.ref.tell(new Crash.RecoveryMsg(node.ref), ActorRef.noSender());
                break;
            }
        }
    }

    /**
     * Debug.SuccessMsg handler; the coordinator receives the success of the operation done in the current
     * round. Based on this information, it performs operations to start the next round in a correct state.
     *
     * @param msg Debug.SuccessMsg message
     */
    private void receiveSuccess(Debug.SuccessMsg msg) {
        ongoing_actions--;
        Peer acting_node = all_nodes.stream().filter(p->p.ref.equals(msg.node)).findAny().get();
        if (acting_node == null) {
            System.out.println("ACTING NODE IS NULL?!?!");
        }
        System.out.println("<<<< SUCCESS " + acting_node.id + " " + msg.op);

        switch (msg.op) {
            case JOIN:
            nodes_out.remove(acting_node);
            nodes_in.add(acting_node);
            break;

            case LEAVE:
            nodes_in.remove(acting_node);
            nodes_out.add(acting_node);
            break;

            case CRASH:
            crashed_nodes.add(acting_node);
            break;

            case RECOVER:
            crashed_nodes.remove(acting_node);
            break;

            default:
            break;
        }

        // when all the operations of the current round are finished, the coordinator waits until all the remaining
        // messages are sent/received, then it starts a new round
        if (ongoing_actions<=0) {
            try { Thread.sleep(2*App.MSG_MAX_DELAY); }catch(Exception e) { System.out.println(e.getMessage()); }
            System.out.println("\\\\\\\\\\ ROUND END");
            getSelf().tell(new Debug.StartRoundMsg(), getSelf());
        }
    }

    /**
     * Debug.FailMsg handler; the coordinator is informed about the failure of the operation done in the current round,
     * and it starts a new round when all remaining operations/messages are completed.
     *
     * @param msg
     */
    private void receiveFail(Debug.FailMsg msg) {
        ongoing_actions--;
        Peer acting_node = all_nodes.stream().filter(p->p.ref.equals(msg.node)).findAny().get();
        if (acting_node == null) {
            System.out.println("ACTING NODE IS NULL?!?!");
        }
        System.out.println("<<<< FAIL " + acting_node.id + " " + msg.op);
        if (ongoing_actions<=0) {
            try { Thread.sleep(2*App.MSG_MAX_DELAY); }catch(Exception e) { System.out.println(e.getMessage()); }
            System.out.println("\\\\\\\\\\ ROUND END");
            getSelf().tell(new Debug.StartRoundMsg(), getSelf());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
        .match(Debug.AddNodesMsg.class, this::receiveAddNodesMsg)
        .match(Debug.AddClientMsg.class, this::receiveAddClientMsg)
        .match(Debug.FailMsg.class, this::receiveFail)
        .match(Debug.SuccessMsg.class, this::receiveSuccess)
        .match(Debug.StartRoundMsg.class, this::receiveStartRoundMsg)
        .build();
    }

    /**
     * It updates the keys of the system using as bound the maximum ID of all the nodes inserted in the system.
     *
     * @param peers list of peers
     */
    private void updateKeys(List<Peer> peers){

        for (Peer p: peers) {
            if (p.id > max_id) max_id = p.id;
        }

        for (int i=0; i<K; i++){
            keys[i]=rng.nextInt(max_id+10);
        }
    }
}
