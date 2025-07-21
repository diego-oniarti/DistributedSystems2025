package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;

import org.example.Node.Peer;
import org.example.msg.Get;
import org.example.msg.Join;
import org.example.msg.Set;
import org.example.shared.Graph;
import org.example.shared.NamedClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

import static org.example.App.*;

public class AppDebug {

    public static final int N_SET = 10;
    public static final int N_CRASH = 0;
    public static final int N_JOIN = 0;
    public static final int N_LEAVE = 0;

    public static final int STARTING_NODES = 5;
    public static final int N_CLIENT = 2;

    public ActorSystem system;
    public List<NamedClient> clients;
    public List<Peer> nodes;

    public AppDebug(String name) {
        this.system = ActorSystem.create(name);
        this.clients = new ArrayList<>();
        this.nodes = new ArrayList<>();

        this.addClients();
        this.addNodes();
    }

    public void addClients() {
        for (int i = 0; i<N_CLIENT; i++){
            String name = generateRandomString(4);
            this.clients.add(new NamedClient(name, this.system.actorOf(Client.props(name))));
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
                .tell(new Set.InitiateMsg(key, value), this.clients.get(rng.nextInt(clients.size())).ref);

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
            // Find items' location in the ring
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

    public void sequentialConsistencyTest() {
        // setting of the network
        Random rng = new Random();

        // create debug file and direct the console output to it
        PrintStream console = System.out;
        try{
            PrintStream fileOut = new PrintStream(new FileOutputStream("seq_cons.txt"));
            System.setOut(fileOut);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        final int N_OP = 20;

        int bound = (STARTING_NODES+1)*10;

        int[] keys = new int[4];
        for (int i=0; i<keys.length; i++) {
            keys[i] = rng.nextInt(bound);
        }

        for (int i=0; i<N_OP; i++) {
            ActorRef client = clients.get(rng.nextInt(clients.size())).ref;
            Peer node = nodes.get(rng.nextInt(nodes.size()));
            int key = keys[rng.nextInt(keys.length)];

            if (rng.nextBoolean()) {
                // SET
                String value = generateRandomString(5);
                node.ref.tell(new Set.InitiateMsg(key, value), client);
            }else{
                // GET
                node.ref.tell(new Get.InitiateMsg(key), client);
            }
            try { Thread.sleep((long)(T*3)); } catch (InterruptedException e) {e.printStackTrace(); }
        }


        try { Thread.sleep(T+1); } catch (InterruptedException e) {e.printStackTrace(); }
        System.setOut(console);
        try {
            console.println(">> Press Enter to End <<");
            System.in.read();
        }catch (Exception e) {}
        this.system.terminate();
    }

     public String check_consistency_file() {
        // Client_name -> [(item_key, item_value)]
        Map<String, List<Pair<Integer, String>>> history = new HashMap<>();
        for (NamedClient client: clients) {
            history.put(client.ref.toString(), new LinkedList<>());
        }

        Graph graph = new Graph();

        try {
            File cons = new File("seq_cons.txt");
            Scanner scan = new Scanner(cons);
            while (scan.hasNextLine()) {        // W node_id item_key version
                if (scan.hasNext("W")) {
                    scan.nextLine();
                } else if (scan.hasNext("R")) {
                    scan.next();
                    String client = scan.next();
                    int node_id = scan.nextInt();
                    Integer item_key = scan.nextInt();
                    String item_value = scan.next();

                    history.get(client).add(new Pair<Integer,String>(item_key, item_value));

                    graph.addNode(item_key.toString()+"."+item_value);
                } else {
                    String msg = scan.nextLine();
                }

            }
            scan.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (NamedClient client: clients) {
            Map<Integer, String> last_seen = new HashMap<>();
            String cid = client.ref.toString();

            List<Pair<Integer, String>> clientHistory = history.get(client.ref.toString());
            for (Pair<Integer, String> read: clientHistory) {
                int key = read.first();
                String val = read.second();

                String last = last_seen.get(key);
                last_seen.put(key, val);
                if (last==null) {
                    continue;
                }
                if (!last.equals(val)) {
                    graph.addEdge(key+"."+last, key+"."+val);
                }
            }
        }

        List<String> ordering = graph.check_topological_ordering();
        if (ordering==null) {
            return "Sequential inconsistency";
        }

        System.out.println("ORDERING:");
        for (String e: ordering) {
            System.out.print(e+" ");
        }
        System.out.println();

        return "Sequentially consistent";
    }

    public void setDynamicTest(){

        Random rng = new Random();

        // create debug file and direct the console output to it
        PrintStream console = System.out;
        try{
            PrintStream fileOut = new PrintStream(new FileOutputStream("set_dynamic.txt"));
            System.setOut(fileOut);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        final int N_OP = 10;
        int bound = (STARTING_NODES+N_OP+1)*10;
        int n_joins = 0;
        List<Peer> old_net = new ArrayList<>();
        old_net.addAll(nodes);

        for (int i=0; i<N_OP; i++) {

            Peer node = old_net.get(rng.nextInt(old_net.size()));

            if (rng.nextBoolean()) {
                // SET
                int key = rng.nextInt(bound);
                String value = generateRandomString(5);
                ActorRef client = clients.get(rng.nextInt(clients.size())).ref;
                node.ref.tell(new Set.InitiateMsg(key, value), client);
            }else{
                // JOIN
                Peer joining_node = new Peer((STARTING_NODES+n_joins)*10,this.system.actorOf(Node.props((STARTING_NODES+n_joins)*10)));
                nodes.add(joining_node);
                node.ref.tell(new Join.InitiateMsg(), joining_node.ref);
                n_joins++;
            }
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

    public String check_dynamic_set_file(){

        // read the file and check it
        Map<Integer, Integer> items = new HashMap<>();      // Key -> latest version
        Map<Integer, Map<Integer, Integer>> storage = new HashMap<>(); // node_id -> (key -> last version)

        for (Peer p : nodes) {
            storage.put(p.id, new HashMap<>());
        }

        // TODO: avoid duplicated code
        try {
            File set = new File("set_dynamic.txt");
            Scanner scan = new Scanner(set);
            while (scan.hasNextLine()) {                // W node_id item_key version
                if (scan.hasNext("W")){
                    scan.next();                        // Discard W
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
                }else if (scan.hasNext("A")) {   //  A node_id item_key version
                    scan.next();                        // Discard W
                    int node_id = scan.nextInt();
                    int item_key = scan.nextInt();
                    int item_version = scan.nextInt();
                    storage.get(node_id).put(item_key, item_version);
                }else if (scan.hasNext("D")) {   //  D node_id item_key
                    scan.next();                        // Discard W
                    int node_id = scan.nextInt();
                    int item_key = scan.nextInt();
                    storage.get(node_id).remove(item_key);
                }else{
                    scan.nextLine();
                }
            }
            scan.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // check data items assignment
        for (int item_key : items.keySet()){
            // Find items' location in the ring
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
                }else{
                    // remove the element if present
                    storage.remove(responsible_id).get(item_key);
                }
            }
        }

        if (!storage.isEmpty()){
            return "Some node has data items it is no longer responsible for";
        }

        return "Dynamic set test is correct";
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

/*
 * TODO
 * 1. Modify all tests after having managed clients problem
 */
