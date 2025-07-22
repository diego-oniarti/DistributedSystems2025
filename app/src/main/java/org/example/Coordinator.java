package org.example;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import org.example.msg.*;
import org.example.msg.Set;

import java.util.*;

import static org.example.App.ROUNDS;

public class Coordinator extends AbstractActor {

    private final static int K = 4;

    private List<ActorRef> clients;
    private List<ActorRef> nodes_in;
    private List<ActorRef> nodes_out;
    private List<ActorRef> crashed_nodes;

    private int max_id;
    private int [] keys;

    private int ongoing_action;
    private int current_round;

    private Random rng;

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

    private void receiveAddClientMsg(Debug.AddClientMsg msg){
        this.clients.add(msg.ref);
    }

    private void receiveAddNodeMsg(Debug.AddNodeMsg msg){

        if (rng.nextFloat()<0.75){
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

    private void receiveStartRoundMsg(Debug.StartRoundMsg msg){

        // READ and WRITE
        if (rng.nextBoolean()){
            ongoing_action = clients.size();

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
                case 1:
                    if (nodes_out.isEmpty()) { getSelf().tell(new Debug.DecreaseOngoingMsg(),getSelf()); return; }
                    ActorRef new_node = randomOutNode();
                    nodes_in.add(new_node);
                    ongoing_action = nodes_in.size()-crashed_nodes.size();

                    node.tell(new Join.InitiateMsg(), new_node);
                    break;
                case 2:
                    nodes_in.remove(node);

                    node.tell(new Leave.InitiateMsg(),ActorRef.noSender());
                    break;
                case 3:
                    if (crashed_nodes.size()==nodes_in.size()-1) { getSelf().tell(new Debug.DecreaseOngoingMsg(),getSelf()); return; }
                    crashed_nodes.add(node);
                    ongoing_action = 1;
                    node.tell(new Crash.InitiateMsg(),ActorRef.noSender());
                    break;
                case 4:
                    while (crashed_nodes.contains(node)){
                        node = randomNode();
                    }
                    ActorRef crashed_node = randomCrashedNode();
                    crashed_node.tell(new Crash.RecoveryMsg(node),ActorRef.noSender());
                    break;
            }
        }
    }

    private void receiveIncreaseOngoingMsg (Debug.IncreaseOngoingMsg msg){
        if (!crashed_nodes.contains(msg.responsible)){
            ongoing_action++;
        }
    }

    private void receiveDecreaseOngoingMsg(Debug.DecreaseOngoingMsg msg){
        this.ongoing_action--;
        if (this.ongoing_action==0 && this.current_round++<ROUNDS){
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

    private ActorRef randomNode(){
        return nodes_in.get(rng.nextInt(nodes_in.size()));
    }

    private ActorRef randomOutNode(){
        return nodes_out.remove(rng.nextInt(nodes_out.size()));
    }

    private ActorRef randomCrashedNode(){
        return crashed_nodes.remove(rng.nextInt(crashed_nodes.size()));
    }
}
