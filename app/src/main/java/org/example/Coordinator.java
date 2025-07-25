package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import org.example.msg.*;
import org.example.msg.Set;

import java.util.*;

import static org.example.App.*;

/**
 * The class represents a coordinator that perform a simulation providing project assumptions and requirements.
 */
public class Coordinator extends AbstractActor {

    /// ATTRIBUTES

    /** Number of keys */
    private final static int K = 4;

    /** List of clients */
    private List<ActorRef> clients;
    /** List of nodes in the system */
    private List<ActorRef> nodes_in;
    /** List of nodes out from the system */
    private List<ActorRef> nodes_out;
    /** List of crashed nodes */
    private List<ActorRef> crashed_nodes;

    /** Maximum ID of the nodes (in and out) */
    private int max_id;
    /** Array of keys */
    private int [] keys;

    /** Counter of the actions that need to finish before starting another round */
    private int ongoing_action;
    /** Number of current round */
    private int current_round;

    private Random rng;

    /// CONSTRUCTOR

    /**
     * Constructor of the Coordinator class.
     */
    public Coordinator() {
        this.clients = new ArrayList<>();
        this.nodes_in = new ArrayList<>();
        this.nodes_out = new ArrayList<>();
        this.crashed_nodes = new ArrayList<>();
        this.max_id = 0;
        this.keys = new int[K];
        this.ongoing_action = 0;
        this.current_round = 0;
        this.rng = new Random();
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
        this.clients.add(msg.ref);
    }

    /**
     * Handler of Debug.AddNodeMsg; it adds the node (in or out) and generates keys considering as bound the maximum
     * ID plus ten.
     *
     * @param msg Debug.AddNodeMsg message
     */
    private void receiveAddNodeMsg(Debug.AddNodeMsg msg){

        if (nodes_in.size()<N || rng.nextFloat()<0.75){
            this.nodes_in.add(msg.ref);
        }else{
            this.nodes_out.add(msg.ref);
        }

        if (msg.id>max_id){
            max_id = msg.id;

            for (int i=0; i<K; i++){
                keys[i]=rng.nextInt(max_id+10);
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

        // READ and WRITE
        if (rng.nextBoolean()){
            ongoing_action = clients.size();
            System.out.println(("SET/GET (round "+this.current_round+")"));
            for (ActorRef client : clients){
                int key = keys[rng.nextInt(K)];
                ActorRef node = randomNode();
                // WRITE
                if (rng.nextBoolean()){
                    node.tell(new Set.InitiateMsg(key, generateRandomString(20)), client);
                }else{ //READ
                    node.tell(new Get.InitiateMsg(key), client);
                }
            }
        }else{ // JOIN, LEAVE, CRASH, RECOVERY
            ActorRef node = randomNode();
            switch (rng.nextInt(4)){
                // JOIN
                case 1:
                    System.out.println("JOIN (round "+this.current_round+")");

                    if (nodes_out.isEmpty()) { getSelf().tell(new Debug.StartRoundMsg(),getSelf()); return; }
                    ActorRef new_node = randomOutNode();
                    nodes_in.add(new_node);
                    ongoing_action = nodes_in.size()-crashed_nodes.size();


                    node.tell(new Join.InitiateMsg(getSelf()), new_node);
                    break;
                // LEAVE
                /*case 2:
                    nodes_in.remove(node);

                    System.out.println("LEAVE");
                    node.tell(new Leave.InitiateMsg(),ActorRef.noSender());
                    break;*/
                // CRASH
                case 3:
                    System.out.println("CRASH (round "+this.current_round+")");
                    // condition to avoid total crash failures
                    if (crashed_nodes.size()==nodes_in.size()-1 || crashed_nodes.contains(node))
                    { getSelf().tell(new Debug.StartRoundMsg(),getSelf()); return; }
                    crashed_nodes.add(node);
                    ongoing_action = 1;


                    node.tell(new Crash.InitiateMsg(),ActorRef.noSender());
                    break;
                // RECOVERY
                case 4:
                    System.out.println("RECOVERY (round "+this.current_round+")");
                    if (crashed_nodes.size()==0) { getSelf().tell(new Debug.StartRoundMsg(),getSelf()); return;}
                    while (crashed_nodes.contains(node)){
                        node = randomNode();
                    }
                    ActorRef crashed_node = randomCrashedNode();

                    crashed_node.tell(new Crash.RecoveryMsg(node),ActorRef.noSender());
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
        if (!crashed_nodes.contains(msg.responsible)){
            ongoing_action++;
        }
    }

    /**
     * Debug.DecreaseOngoingMsg handler; it reduces the ongoing_action parameter and if it reachs zero it starts
     * a new round if it is possible.
     *
     * @param msg Debug.DecreaseOngoingMsg message
     */
    private void receiveDecreaseOngoingMsg(Debug.DecreaseOngoingMsg msg){
        this.ongoing_action--;
        if (this.ongoing_action==0){
            current_round++;
            if (current_round==ROUNDS){return;}
            getSelf().tell(new Debug.StartRoundMsg(),ActorRef.noSender());
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Debug.AddNodeMsg.class, this::receiveAddNodeMsg)
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

    /**
     * It takes a random node from nodes in.
     *
     * @return the random node
     */
    private ActorRef randomNode(){
        return nodes_in.get(rng.nextInt(nodes_in.size()));
    }

    /**
     * It removes a random node from nodes out,
     *
     * @return the random node
     */
    private ActorRef randomOutNode(){
        return nodes_out.remove(rng.nextInt(nodes_out.size()));
    }

    /**
     * It removes a random node from crashed nodes.
     *
     * @return the random node.
     */
    private ActorRef randomCrashedNode(){
        return crashed_nodes.remove(rng.nextInt(crashed_nodes.size()));
    }
}
