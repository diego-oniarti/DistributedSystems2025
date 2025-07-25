package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import org.example.Node.Peer;
import org.example.msg.*;
import org.example.msg.Set;
import org.example.shared.NamedClient;
import org.example.shared.RngList;

import java.util.*;
import java.util.function.Predicate;

import static org.example.App.*;

/**
 * The class represents a coordinator that perform a simulation providing project assumptions and requirements.
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

    // /**
    //  * Handler of Debug.AddNodeMsg; it adds the node (in or out) and generates keys considering as bound the maximum
    //  * ID plus ten.
    //  *
    //  * @param msg Debug.AddNodeMsg message
    //  */
    // private void receiveAddNodeMsg(Debug.AddNodeMsg msg){
    //     if (nodes_in.size()<N || rng.nextFloat()<0.75){
    //         this.nodes_in.add(msg.ref);
    //     }else{
    //         this.nodes_out.add(msg.ref);
    //     }
    //     if (msg.id>max_id){
    //         max_id = msg.id;
    //         for (int i=0; i<K; i++){
    //             keys[i]=rng.nextInt(max_id+10);
    //         }
    //     }
    // }

    private void receiveAddNodesMsg(Debug.AddNodesMsg msg){
        int maxId = 0;
        for (Peer p: msg.peers) {
            if (p.id > maxId) maxId = p.id;
            if (this.nodes_in.size()<App.STARTING_NODES) {
                this.nodes_in.add(p);
            } else {
                this.nodes_out.add(p);
            }
        }
        for (int i=0; i<K; i++){
            keys[i]=rng.nextInt(max_id+10);
        }
        for (Peer n1: this.nodes_in) {
            for (Peer n2: this.nodes_in) {
                n1.ref.tell(new Debug.AddNodeMsg(n2.ref, n2.id), n2.ref);
            }
        }
    }



    /**
     * Debug.StartRoundMsg handler; it chooses between performing a set/get round or a join/leave/crash/recovery round
     * and executes it.
     *
     * @param msg Debug.StartRoundMsg message
     */
    private void receiveStartRoundMsg(Debug.StartRoundMsg msg){
        // try {Thread.sleep(5000); }catch (Exception e) { System.out.println("CRASH"); }
        System.out.println("///// STARTING ROUND "+current_round);
        // try {Thread.sleep(5000); }catch (Exception e) { System.out.println("CRASH"); }

        ongoing_actions = 0;
        // READ and WRITE
        if (rng.nextBoolean()){
            ongoing_actions = clients.size();
            for (NamedClient client : clients){
                int key = keys[rng.nextInt(K)];
                ActorRef node = nodes_in.getRandom().ref;
                if (rng.nextBoolean()) {
                    // WRITE
                    System.out.println(("SET (round "+this.current_round+")"));
                    node.tell(new Set.InitiateMsg(key, generateRandomString(20)), client.ref);
                } else {
                    // READ
                    System.out.println(("GET (round "+this.current_round+")"));
                    node.tell(new Get.InitiateMsg(key), client.ref);
                }
            }
        }else{ // JOIN, LEAVE, CRASH, RECOVERY
            Peer node;
            do { node = nodes_in.getRandom(); }while(crashed_nodes.contains(node));

            switch (rng.nextInt(4)){
                // JOIN
                case 0:
                    System.out.println("JOIN (round "+this.current_round+")");

                    if (nodes_out.isEmpty()) {
                        getSelf().tell(new Debug.StartRoundMsg(), getSelf());
                        return;
                    }
                    Peer new_node = nodes_out.removeRandom();
                    nodes_in.add(new_node);

                    node.ref.tell(new Join.InitiateMsg(), new_node.ref);
                    break;
                // LEAVE
                case 1:
                    System.out.println("LEAVE (round "+this.current_round+")");
                    if (nodes_in.size() == N) {
                        System.out.println("Can't have less than N nodes");
                        getSelf().tell(new Debug.StartRoundMsg(), getSelf());
                        return;
                    }

                    nodes_in.remove(node);
                    node.ref.tell(new Leave.InitiateMsg(), ActorRef.noSender());
                    break;
                // CRASH
                case 2:
                    System.out.println("CRASH (round "+this.current_round+")");

                    // condition to avoid total crash failure || crashed_nodes.contains(node))s
                    if (crashed_nodes.size()==nodes_in.size()-1) {
                        System.out.println("Can't crash only remaining node");
                        getSelf().tell(new Debug.StartRoundMsg(), getSelf());
                        return;
                    }

                    crashed_nodes.add(node);
                    ongoing_actions = 1;

                    node.ref.tell(new Crash.InitiateMsg(),ActorRef.noSender());
                    break;
                // RECOVERY
                case 3:
                    System.out.println("RECOVERY (round "+this.current_round+")");

                    if (crashed_nodes.size()==0) { getSelf().tell(new Debug.StartRoundMsg(),getSelf()); return;}
                    Peer crashed_node = crashed_nodes.removeRandom();
                    nodes_in.add(crashed_node);

                    crashed_node.ref.tell(new Crash.RecoveryMsg(node.ref), ActorRef.noSender());
                    break;
            }
        }
    }

    /**
     * Debug.IncreaseOngoingMsg handler; it increases the ongoing_action parameter if the responsible for the
     * message didn't crash.
     *
     * @param msg Debug.IncreaseOngoingMsg message
     */
    private void receiveIncreaseOngoingMsg (Debug.IncreaseOngoingMsg msg){
        if (crashed_nodes.stream().noneMatch(p -> p.ref==msg.responsible)) {
            ongoing_actions++;
        }
    }

    /**
     * Debug.DecreaseOngoingMsg handler; it reduces the ongoing_action parameter and if it reachs zero it starts
     * a new round if it is possible.
     *
     * @param msg Debug.DecreaseOngoingMsg message
     */
    private void receiveDecreaseOngoingMsg(Debug.DecreaseOngoingMsg msg){
        this.ongoing_actions--;
        if (this.ongoing_actions<=0){
            this.ongoing_actions=0;
            current_round++;
            if (current_round>=ROUNDS){
                System.out.println("> PRESS ENTER <");
                return;
            }
            getSelf().tell(new Debug.StartRoundMsg(), ActorRef.noSender());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Debug.AddNodesMsg.class, this::receiveAddNodesMsg)
            .match(Debug.AddClientMsg.class, this::receiveAddClientMsg)
            .match(Debug.DecreaseOngoingMsg.class, this::receiveDecreaseOngoingMsg)
            .match(Debug.IncreaseOngoingMsg.class, this::receiveIncreaseOngoingMsg)
            .match(Debug.StartRoundMsg.class, this::receiveStartRoundMsg)
            .build();
    }

    /**
     * It generates a random String.
     *
     * @param length length of the String
     * @return the random String
     */
    private String generateRandomString(int length) {
        String characterSet = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characterSet.length());
            sb.append(characterSet.charAt(index));
        }
        return sb.toString();
    }
}
