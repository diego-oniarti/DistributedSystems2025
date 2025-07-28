package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;

import org.example.Node.Peer;
import org.example.msg.Debug;
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
import java.util.stream.IntStream;

import static org.example.App.*;

/**
 * The class executes some tests to check system properties.
 */
public class AppDebug {

    public static final int N_SET = 10;
    public static final int N_CRASH = 0;
    public static final int N_JOIN = 0;
    public static final int N_LEAVE = 0;

    public static final int N_CLIENT = 2;

    public ActorSystem system;
    public List<NamedClient> clients;
    public List<Peer> nodes;
    public List<Peer> nodes_out;
    public ActorRef coordinator;

    public AppDebug(String name){
        this.system = ActorSystem.create(name);
        this.clients = new ArrayList<>();
        this.nodes = new ArrayList<>();
        this.nodes_out = new ArrayList<>();

        this.addCoordinator();
        this.addClients();
        this.addNodes();
    }

    public void addCoordinator(){
        this.coordinator = this.system.actorOf(Coordinator.props());
    }

    public void addClients() {
        for (int i = 0; i<N_CLIENT; i++){
            String name = generateRandomString(4);
            ActorRef client = this.system.actorOf(Client.props(name));
            this.clients.add(new NamedClient(name, client));
            this.coordinator.tell(new Debug.AddClientMsg(client, name), ActorRef.noSender());
            client.tell(new Debug.AnnounceCoordinator(coordinator), ActorRef.noSender());
        }
    }

    public void addNodes(){
        for (int i=0; i<STARTING_NODES+ROUNDS; i++) {
            Peer p = new Peer(i*10, this.system.actorOf(Node.props(i*10)));
            p.ref.tell(new Debug.AnnounceCoordinator(coordinator), ActorRef.noSender());
            if (i<STARTING_NODES){
                this.nodes.add(p);
            }else{
                this.nodes_out.add(p);
            }

        }

        for (Peer n1: this.nodes) {
            for (Peer n2: this.nodes) {
                n1.ref.tell(new Debug.AddNodeMsg(n2.ref, n2.id), n2.ref);
            }
        }

        LinkedList<Peer> test_in = new LinkedList<>();
        test_in.addAll(this.nodes);
        LinkedList<Peer> test_out = new LinkedList<>();
        test_out.addAll(this.nodes_out);
        coordinator.tell(new Debug.AddNodesMsg(test_in,test_out), ActorRef.noSender());
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
                if (scan.hasNext("WRITE")){
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
                if (scan.hasNext("WRITE")) {
                    scan.nextLine();
                } else if (scan.hasNext("READ")) {
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

        coordinator.tell(new Debug.StartRoundMsg(),ActorRef.noSender());

        try { Thread.sleep(T*ROUNDS*N_CLIENT); } catch (InterruptedException e) {e.printStackTrace(); }

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

        int start = nodes.size();
        int n_joins = 0;
        int n_leave = 0;

        try {
            File set = new File("set_dynamic.txt");
            Scanner scan = new Scanner(set);
            int node_id = 0;
            int item_key = 0;
            int item_version = 0;

            while (scan.hasNextLine()) {
                String next = scan.next();
                switch (scan.next()) {
                    case "WRITE":               // WRITE node_id item_key version
                        node_id = scan.nextInt();
                        item_key = scan.nextInt();
                        item_version = scan.nextInt();

                        // Register the new item or update the version
                        boolean item_is_present = items.get(item_key) != null;
                        if (!item_is_present || item_is_present && items.get(item_key) < item_version) {
                            items.put(item_key, item_version);
                        }

                        // Register the new value for the node
                        storage.get(node_id).put(item_key, item_version);
                        break;
                    case "ADD":                 //  ADD node_id item_key version
                        node_id = scan.nextInt();
                        item_key = scan.nextInt();
                        item_version = scan.nextInt();
                        // we register the node that was added to the joining node
                        if (!storage.containsKey(node_id)){
                            storage.put(node_id,new HashMap<>());
                        }
                        storage.get(node_id).put(item_key, item_version);
                        break;
                    case "DELETE":              //  DELETE node_id item_key
                        node_id = scan.nextInt();
                        item_key = scan.nextInt();
                        // we register the node that was removed because a new node joins the network
                        if (!storage.containsKey(node_id)){
                            storage.put(node_id,new HashMap<>());
                        }
                        storage.get(node_id).remove(item_key);
                        break;
                    case "JOINING":             // JOINING node_id
                        n_joins++;
                        node_id=scan.nextInt();

                        // find the index of the joining node in the nodes out list
                        int joining_index = 0;
                        for (Peer p: nodes_out){
                            if (p.id!=node_id){
                                joining_index++;
                            }else{
                                break;
                            }
                        }

                        // if there is, we insert the joining node to the nodes list maintaining the order, needed for
                        // checks
                        if (joining_index!=nodes_out.size()){
                            Peer joining_node = nodes_out.remove(joining_index);
                            int i=0;
                            while (i<nodes.size() && joining_node.id > nodes.get(i).id) {
                                i++;
                            }
                            nodes.add(i, joining_node);
                        }

                        break;
                    case "LEAVE":
                        n_leave++;
                        node_id= scan.nextInt();

                        // find the index of the leaving node in the nodes list
                        int leave_index = 0;
                        for (Peer p: nodes){
                            if (p.id!=node_id){
                                leave_index++;
                            }else{
                                break;
                            }
                        }

                        // if there is, we insert the leaving node to the nodes out list
                        if (leave_index!=nodes.size()){
                            Peer leaving_node = nodes.remove(leave_index);
                            this.nodes_out.add(leaving_node);
                            storage.remove(leaving_node.id);
                        }

                    default:
                        scan.nextLine();
                }
            }
            scan.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // check number of nodes
        if (nodes.size()!=start+(n_joins-n_leave)){
            return "Final number of nodes in uncorrected";
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

        // if we have some data items left in the storage this means that some nodes hold data items they are no
        // responsible for
        for (int k : storage.keySet()){
            if (!storage.get(k).isEmpty()){
                return "Some node has data items it is no longer responsible for";
            }
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
