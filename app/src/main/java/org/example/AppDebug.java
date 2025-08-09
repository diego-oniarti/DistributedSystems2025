package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import scala.collection.immutable.HashSet;

import org.example.Node.Entry;
import org.example.Node.Peer;
import org.example.msg.Debug;
import org.example.msg.Get;
import org.example.msg.Set;
import org.example.shared.Graph;
import org.example.shared.NamedClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.example.App.*;

/**
 * The class executes some tests to check system properties.
 */
public class AppDebug {

    /** Number of set operations for fixed network test set */
    public static final int N_SET = 10;

    /** Number of clients in the system */
    public static final int N_CLIENT = 2;

    /** System */
    public ActorSystem system;
    /** List of clients */
    public List<NamedClient> clients;
    /** List of nodes in the network */
    public List<Peer> nodes_in;
    /** List of nodes out of the network */
    public List<Peer> nodes_out;
    /** Coordinator */
    public ActorRef coordinator;

    /** Constructor of the AppDebug class; it populates the system. */
    public AppDebug(String name){
        this.system = ActorSystem.create(name);
        this.clients = new ArrayList<>();
        this.nodes_in = new ArrayList<>();
        this.nodes_out = new ArrayList<>();

        this.coordinator = this.system.actorOf(Coordinator.props());
        this.addClients();
        this.addNodes();
    }

    /** It creates and adds clients to the system informing the coordinator. */
    public void addClients() {
        for (int i = 0; i<N_CLIENT; i++){
            String name = generateRandomString(4);
            ActorRef client = this.system.actorOf(Client.props(name));
            this.clients.add(new NamedClient(name, client));
            this.coordinator.tell(new Debug.AddClientMsg(client, name), ActorRef.noSender());
            client.tell(new Debug.AnnounceCoordinator(this.coordinator), ActorRef.noSender());
        }
    }

    /** In creates and add nodes (in and out) to the system informing the coordinator. */
    public void addNodes(){
        for (int i=0; i<STARTING_NODES+ROUNDS; i++) {
            Peer p = new Peer(i*10, this.system.actorOf(Node.props(i*10)));
            p.ref.tell(new Debug.AnnounceCoordinator(this.coordinator), ActorRef.noSender());
            if (i<STARTING_NODES){
                this.nodes_in.add(p);
            }else{
                this.nodes_out.add(p);
            }
        }

        for (Peer n1: this.nodes_in) {
            for (Peer n2: this.nodes_in) {
                n1.ref.tell(new Debug.AddNodeMsg(n2.ref, n2.id), n2.ref);
            }
        }

        LinkedList<Peer> test_in = new LinkedList<>();
        test_in.addAll(this.nodes_in);
        LinkedList<Peer> test_out = new LinkedList<>();
        test_out.addAll(this.nodes_out);
        coordinator.tell(new Debug.AddNodesMsg(test_in,test_out), ActorRef.noSender());
    }

    /**
     * It performs a fixed network set test; it generates random set operations redirecting the out stream in a
     * file "set.txt".
     */
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
            this.nodes_in.get(rng.nextInt(nodes_in.size())).ref
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

    /**
     * Checks the set.txt file; it sees if the assignment of data items is correct and if the number of operations
     * executed is correct.
     *
     * @return a String explaining errors or success
     */
    public String check_set_file() {

        // items inserted in the system
        Map<Integer, Integer> items = new HashMap<>();      // Key -> latest version
        // items inserted in nodes local storage
        Map<Integer, Map<Integer, Integer>> storage = new HashMap<>(); // node_id -> (key -> last version)

        for (Peer p : nodes_in) {
            storage.put(p.id, new HashMap<>());
        }

        int n_fails = 0;

        try {
            File set = new File("set.txt");
            Scanner scan = new Scanner(set);
            while (scan.hasNextLine()) {        // WRITE node_id item_key version
                if (scan.hasNext("WRITE")){
                    scan.next();                // Discard WRITE
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
                    // update the number of fails
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
            while (index < storage.size() && nodes_in.get(index).id < item_key){
                index++;
            }
            if (index == storage.size()) { index = 0; }

            // Check if all the responsibles have the item
            for (int i = 0; i<N; i++){
                int responsible_id = nodes_in.get((index+i)%nodes_in.size()).id;
                if (storage.get(responsible_id).get(item_key) == null) {
                    return "Node " + responsible_id + " has no item " + item_key;
                }
            }
        }

        return "Set test is correct";

    }

    /**
     * This test performs set/get operations to evaluate sequential consistency. It redirects the out stream to a file
     * "seq_cons.txt".
     */
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
            Peer node = nodes_in.get(rng.nextInt(nodes_in.size()));
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

    /**
     * It checks the file "seq_cons.txt" to see if the system follows sequential consistency. It creates a directed graph
     * with nodes "itemkey.itemvalue" (where item value is the version). A node represents a read operation.
     * Edges are from a node to the successive read operation that a client performed. To check sequential consistency,
     * we check the existence of a topological order between nodes. If it is not so, the sequential consistency isn't
     * provided.
     *
     * @return a String explaining errors or success
     */
    public String check_consistency_file() {
        // read operations of clients
        Map<String, List<Pair<Integer, Pair<String,Integer>>>> history = new HashMap<>();  // Client_name -> [(item_key, <item_value, item_version>)]
        for (NamedClient client: clients) {
            history.put(client.ref.toString(), new LinkedList<>());
        }

        Graph graph = new Graph();

        try {
            File cons = new File("seq_cons.txt");
            Scanner scan = new Scanner(cons);
            while (scan.hasNextLine()) {        // WRITE node_id item_key version
                if (scan.hasNext("READ")) { // READ client_ref node_id item_key version
                    scan.next();
                    String client = scan.next();
                    int node_id = scan.nextInt();
                    Integer item_key = scan.nextInt();
                    String item_value = scan.next();
                    int item_version = scan.nextInt();

                    if (history.get(client) != null) { // Ignore gets made by nodes (for join operation)
                        history.get(client).add(new Pair<>(item_key, new Pair<>(item_value,item_version)));
                    }

                    // create node
                    graph.addNode(item_key.toString()+"."+item_value+"."+item_version);
                } else {
                    scan.nextLine();
                }

            }
            scan.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        for (NamedClient client: clients) {
            Map<Integer, Pair<String,Integer>> last_seen = new HashMap<>();
            String cid = client.ref.toString();

            List<Pair<Integer, Pair<String,Integer>>> clientHistory = history.get(client.ref.toString());
            for (Pair<Integer, Pair<String,Integer>> read: clientHistory) {
                int key = read.first();
                String val = read.second().first();
                int version = read.second().second();

                Pair<String,Integer> last_entry = last_seen.get(key);
                last_seen.put(key, new Pair<>(val,version));
                if (last_entry==null) {
                    continue;
                }
                if (!(last_entry.first().equals(val) && last_entry.second().equals(version))) { // Avoid self-loops
                    // create edge
                    graph.addEdge(key+"."+last_entry.first()+"."+last_entry.second(), key+"."+val+"."+version);
                }
            }
        }

        // check topological order
        List<String> ordering = graph.check_topological_ordering();
        if (ordering==null) {
            return "Sequential inconsistency";
        }

        // print order if found
        System.out.println("ORDERING:");
        for (String e: ordering) {
            System.out.print(e+" ");
        }
        System.out.println();

        for (HashMap.Entry<String, List<Pair<Integer, Pair<String,Integer>>>> e: history.entrySet()) {
            String client = e.getKey();
            System.out.print(client+": ");
            for (Pair<Integer, Pair<String,Integer>> read: e.getValue()) {
                System.out.print(read.first()+"."+read.second().first()+"."+read.second().second()+" - ");
            }
            System.out.println();
        }

        return "Sequentially consistent";
    }

    /**
     * It performs the round simulation with the Coordinator. It redirects the out stream to a file.
     *
     * @param file name of the file to which redirect the out stream
     */
    public void dynamicTest(String file){

        Random rng = new Random();

        // create debug file and direct the console output to it
        PrintStream console = System.out;
        try{
            PrintStream fileOut = new PrintStream(new FileOutputStream(file));
            System.setOut(fileOut);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        coordinator.tell(new Debug.StartRoundMsg(),ActorRef.noSender());

        try {
            System.in.read();
        }catch (Exception e) {}
        System.setOut(console);
        this.system.terminate();

    }

    /**
     * It checks assignment of data items and reassignment of data items in presence of join and leave operations.
     *
     * @return a String explaining errors or success
     */
    public String check_dynamic_file(){
        // items inserted in the system
        Map<Integer, Integer> items = new HashMap<>();      // Key -> latest version
        // items inserted in nodes local storage
        Map<Integer, Map<Integer, Integer>> storage = new HashMap<>(); // node_id -> (key -> last version)

        for (Peer p : nodes_in) {
            storage.put(p.id, new HashMap<>());
        }

        int start = nodes_in.size();
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
                    // when a joining node/node inserts a data item it is now responsible for
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
                    // when a node deletes a data item the joining node is now responsible for
                    case "DELETE":              //  DELETE node_id item_key
                    node_id = scan.nextInt();
                    item_key = scan.nextInt();
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
                        while (i<nodes_in.size() && joining_node.id > nodes_in.get(i).id) {
                            i++;
                        }
                        nodes_in.add(i, joining_node);
                    }

                    break;
                    case "LEAVE":               // LEAVE node_id
                    n_leave++;
                    node_id= scan.nextInt();

                    // find the index of the leaving node in the nodes list
                    int leave_index = 0;
                    for (Peer p: nodes_in){
                        if (p.id!=node_id){
                            leave_index++;
                        }else{
                            break;
                        }
                    }

                    // if there is, we insert the leaving node to the nodes out list
                    if (leave_index!=nodes_in.size()){
                        Peer leaving_node = nodes_in.remove(leave_index);
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
        if (nodes_in.size()!=start+n_joins-n_leave){
            return "Final number of nodes in uncorrected";
        }

        // check data items assignment
        for (int item_key : items.keySet()){
            // Find items' location in the ring
            int index = 0;
            while (index < storage.size() && nodes_in.get(index).id < item_key){
                index++;
            }
            if (index == storage.size()) { index = 0; }

            // Check if all the responsibles have the item
            for (int i = 0; i<N; i++){
                int responsible_id = nodes_in.get((index+i)%nodes_in.size()).id;
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

    public String check_round_sim(){
        Map<Integer, Map<Integer, Entry>> storages  = new HashMap<>();
        Map<Integer, Map<Integer, Entry>> simulated = new HashMap<>();

        HashSet<Integer> crashed_nodes_sim = new HashSet<>();

        /* TODO
        * Put all nodes storages. We'll only keep track of the contents and not the
        * configuration.
        * We'll still keep track of the configuration of the simulated system
        */

        List<Integer> nodes_in_sim  = new LinkedList<>();
        List<Integer> nodes_out_sim = new LinkedList<>();

        for (Peer p: nodes_in) {
            storages.put(p.id, new HashMap<>());
            simulated.put(p.id, new HashMap<>());
            nodes_in_sim.add(p.id);
        }

        for (Peer p: nodes_out) {
            nodes_out_sim.add(p.id);
        }


        try {
            File set = new File("set_dynamic.txt");
            Scanner scan = new Scanner(set);

            while (scan.hasNextLine()) {
                switch (scan.next()) {
                    case "\\\\\\\\\\":
                    List<Integer> all_node_ids_in = Stream.concat(
                        storages.keySet().stream(),
                        simulated.keySet().stream()
                    ).distinct().collect(Collectors.toList());
                    for (int node_id: all_node_ids_in) {
                        if (!(storages.containsKey(node_id) && simulated.containsKey(node_id))) {
                            return "Node " + node_id + " is not in both maps";
                        }
                        Map<Integer, Entry> node_storage = storages.get(node_id);
                        Map<Integer, Entry> node_storage_sim = simulated.get(node_id);

                        List<Integer> all_keys = Stream.concat(
                            node_storage.keySet().stream(),
                            node_storage_sim.keySet().stream()
                        ).distinct().collect(Collectors.toList());
                        for (int key: all_keys) {
                            if (!(node_storage.containsKey(key) && node_storage_sim.containsKey(key))) {
                                return "Key " + key + " is not in both maps";
                            }
                            if (!node_storage.get(key).equals(node_storage_sim.get(key))) {
                                return "Key " + key + " items are different";
                            }
                        }
                    }
                    break;

                    case "ADD":
                    int add_id = scan.nextInt();
                    int add_key = scan.nextInt();
                    String add_val = scan.next();
                    int add_version = scan.nextInt();

                    storages.get(add_id).put(add_key, new Entry(add_val, add_version));
                    break;

                    case "DELETE":
                    int del_id = scan.nextInt();
                    int del_key = scan.nextInt();
                    storages.get(del_id).remove(del_key);
                    break;

                    case "SET":
                    break;

                    case "GET":
                    break;

                    case "JOIN":
                    break;

                    case "LEAVE":
                    break;

                    case "CRASH":
                    break;

                    case "RECOVERY":
                    break;

                    default:
                    scan.nextLine();
                    break;
                };
            }
            scan.close();
        }catch(Exception e) {
        }

        return "ALL IS GOOD IN THE WORLD";
    }

    private List<Integer> getResponsibles(List<Integer> nodes_in, int key) {
        // TODO
    }

    private boolean isResponsible(List<Integer> nodes_in, int id, int key) {
        // TODO
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
