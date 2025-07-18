package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import org.example.Node.Peer;
import org.example.msg.Set;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

import static org.example.App.*;

public class AppDebug {

    public static final int N_SET = 40;
    public static final int N_CRASH = 0;

    public static final int STARTING_NODES = 5;
    public static final int N_CLIENT = 2;

    public ActorSystem system;
    public List<ActorRef> clients;
    public List<Peer> nodes;

    public AppDebug(String name) {
        this.system = ActorSystem.create(name);
        this.clients = new ArrayList<>();
        this.nodes = new ArrayList<>();
    }

    public void addClients(){
        for (int i = 0; i<N_CLIENT; i++){
            this.clients.add(this.system.actorOf(Client.props(generateRandomString(4))));
        }
    }

    public void addNodes(){
        for (int i=0; i<STARTING_NODES; i++) {
            this.nodes.add(new Peer(i*10, this.system.actorOf(Node.props(i*10))));
        }

        for (Peer n1: this. nodes) {
            for (Peer n2: this.nodes) {
                n1.ref.tell(new Node.DebugAddNodeMsg(n2.ref, n2.id), n2.ref);
            }
        }
    }

    public void setFixedTest(){

        // setting of the network
        Random rng = new Random();

        this.addClients();
        this.addNodes();

        // create debug file and direct the console output to it
        PrintStream console = System.out;
        try{
            PrintStream fileOut = new PrintStream(new FileOutputStream("set.txt"));
            System.setOut(fileOut);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // generate random data items and send a set message; wait until the messages are finished
        int bound = (STARTING_NODES+1)*10;
        for (int i = 0; i<N_SET; i++){
            int key = rng.nextInt(bound);
            String value = generateRandomString(3);
            this.nodes.get(rng.nextInt(nodes.size())).ref
                .tell(new Set.InitiateMsg(key, value), this.clients.get(rng.nextInt(clients.size())));

            try { Thread.sleep((long)(T*1.5)); } catch (InterruptedException e) {e.printStackTrace(); }
        }

        try { Thread.sleep(T+1); } catch (InterruptedException e) {e.printStackTrace(); }

        System.setOut(console);
        try {
            console.println(">> Press Enter to End <<");
            System.in.read();
        }catch (Exception e) {}
        this.system.terminate();
    }

    public String check_set_file() {
        // read the file and check it
        Map<Integer, Integer> items = new HashMap<>();      // Key -> latest version
        Map<Integer, Map<Integer, Integer>> storage = new HashMap<>(); // node_id -> (key -> last version)

        for (Peer p : nodes) {
            storage.put(p.id, new HashMap<>());
        }

        int n_fails = 0;

        try {
            File set = new File("set.txt");
            Scanner scan = new Scanner(set);
            while (scan.hasNextLine()) {        // W node_id item_key version
                if (scan.hasNext("W")){
                    scan.next();                // Discard W
                    int node_id = scan.nextInt();
                    int item_key = scan.nextInt();
                    int item_version = scan.nextInt();

                    // Register the new item or update the version
                    boolean item_is_present = items.get(item_key)!=null;
                    if (!item_is_present || item_is_present && items.get(item_key) < item_version){
                        items.put(item_key, item_version);
                    }

                    // Register the new value for the node
                    storage.get(node_id).put(item_key, item_version);
                } else{
                    String msg = scan.nextLine();
                    if (msg.contains("Fail")){
                        n_fails++;
                    }
                }
            }
            scan.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // check number of data items
        int successful_writes = 0;
        for (int version: items.values()) {
            successful_writes += version;
        }
        if (successful_writes != N_SET-n_fails){
            return "Number of data items uncorrected";
        }

        // check data items assignment
        for (int item_key : items.keySet()){
            // Find items's location in the ring
            int index = 0;
            while (index < storage.size() && nodes.get(index).id < item_key){
                index++;
            }
            if (index == storage.size()) { index = 0; }

            // Check if all the responsibles have the item
            for (int i = 0; i<N; i++){
                int responsible_id = nodes.get((index+i)%nodes.size()).id;
                if (storage.get(responsible_id).get(item_key) == null) {
                    return "Node " + responsible_id + " has no item " + item_key;
                }
            }
        }

        return "Set test is correct";

    }

    public String generateRandomString(int length) {
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
