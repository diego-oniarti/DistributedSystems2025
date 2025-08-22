package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;

import org.example.Node.Peer;
import org.example.msg.Debug;
import org.example.shared.Entry;
import org.example.shared.Graph;
import org.example.shared.NameGenerator;
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
            String name = NameGenerator.getName();
            ActorRef client = this.system.actorOf(Client.props(name));
            this.clients.add(new NamedClient(name, client));
            this.coordinator.tell(new Debug.AddClientMsg(client, name), ActorRef.noSender());
            client.tell(new Debug.AnnounceCoordinator(this.coordinator), ActorRef.noSender());
        }
    }

    /** It creates and add nodes (in and out) to the system informing the coordinator. */
    public void addNodes(){
        for (int i=0; i<STARTING_NODES*2; i++) {
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
     * It checks the file "seq_cons.txt" to see if the system follows sequential consistency. It creates a directed graph
     * with nodes "itemkey.itemvalue.itemversion". A node represents a read operation.
     * Edges are from a node to the successive read operation that a client performed on the same data item.
     * To check sequential consistency, we check the existence of a topological order between nodes.
     * If it is not so, the sequential consistency isn't provided.
     *
     * @return a String explaining errors or success
     */
    public String check_consistency_file() {
        // read operations of clients
        Map<String, List<Pair<Integer, Pair<Character, Entry>>>> history = new HashMap<>();  // Client_name -> [ <item_key, <read, <item_value, item_version>>> ]
        for (NamedClient client: clients) {
            history.put(client.ref.toString(), new LinkedList<>());
        }

        Graph graph = new Graph();

        try {
            File cons = new File("seq_cons.txt");
            Scanner scan = new Scanner(cons);
            while (scan.hasNextLine()) {
                if (scan.hasNext("READ")) { // READ client_ref node_id item_key version
                    scan.next();
                    String client = scan.next();
                    int node_id = scan.nextInt();
                    Integer item_key = scan.nextInt();
                    String item_value = scan.next();
                    int item_version = scan.nextInt();

                    if (history.get(client) != null) { // Ignore gets made by nodes (for join operation)
                        history.get(client).add(new Pair<>(item_key, new Pair<>('r', new Entry(item_value,item_version))));

                        // create node
                        String tmp = item_key.toString()+"."+item_value+"."+item_version;
                        graph.addNode("R"+tmp);
                        graph.addEdge("W"+tmp, "R"+tmp); // Reads must happen after writes
                    }
                }else if (scan.hasNext("WRITE")) {
                    scan.next();
                    String client = scan.next();
                    Integer item_key = scan.nextInt();
                    String item_value = scan.next();
                    int item_version = scan.nextInt();

                    if (history.get(client) != null) { // Ignore gets made by nodes (for join operation)
                        history.get(client).add(new Pair<>(item_key, new Pair<>('w', new Entry(item_value, item_version))));
                        String tmp = item_key.toString()+"."+item_value+"."+item_version;
                        graph.addNode("W"+tmp);

                        // Read your writes
                        graph.addNode("R"+tmp); // Imaginary read operation created to enforce this rule
                        graph.addEdge(
                            "W"+tmp,
                            "R"+tmp
                        );
                        history.get(client).add(new Pair<>(item_key, new Pair<>('r', new Entry(item_value, item_version))));
                    }

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

            List<Pair<Integer, Pair<Character, Entry>>> clientHistory = history.get(client.ref.toString());
            for (Pair<Integer, Pair<Character, Entry>> entry: clientHistory) {
                int key = entry.first();
                Character rw = entry.second().first();
                String val = entry.second().second().value;
                int version = entry.second().second().version;

                if (rw=='r') {
                    Pair<String,Integer> last_entry = last_seen.get(key);
                    last_seen.put(key, new Pair<>(val,version));
                    if (last_entry==null) {
                        continue;
                    }
                    if (!(last_entry.first().equals(val) && last_entry.second().equals(version))) { // Avoid self-loops
                        // create edge
                        graph.addEdge("R"+key+"."+last_entry.first()+"."+last_entry.second(), "R"+key+"."+val+"."+version);
                    }
                } else {
                    for (HashMap.Entry<Integer, Pair<String, Integer>> e: last_seen.entrySet()) {
                        graph.addEdge(
                            "R"+e.getKey()+"."+e.getValue().first()+"."+e.getValue().second(),
                            "W"+key+"."+val+"."+version
                        );

                    }
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
        System.out.print(ordering.stream().collect(Collectors.joining(" - ")));
        System.out.println();

        for (HashMap.Entry<String, List<Pair<Integer, Pair<Character, Entry>>>> e: history.entrySet()) {
            // Iterate through the NamesClients, filter out the one with the same ref as this entry, get its name
            String client_name = clients.stream()
            .filter(c->c.ref.toString().equals(e.getKey()))
            .findAny().map(nc->nc.name).orElse("name_unknown");

            System.out.print("\n"+client_name+": ");
            // for (Pair<Integer, Pair<Character, Entry>> read: e.getValue()) {
            //     System.out.print(read.second().first().toString()+read.first()+"."+read.second().second().value+"."+read.second().second().version+" - ");
            // }
            // Same print as above, but it does not pring a dash at the end of the line
            System.out.println(e.getValue().stream().map(r->
                r.second().first().toString()+r.first()+"."+r.second().second().value+"."+r.second().second().version
            ).collect(Collectors.joining(" - ")));
        }

        return "Sequentially consistent";
    }

    /**
     * It checks that round system data items assignment is correct checking the file "seq_cons.txt" containing
     * the simulation output. It writes the execution better to the console.
     *
     * @return a String explaining errors or success
     */
    public String check_round_sim(){
        /** Storages of all the nodes in the system. Mirrors the state of the system as described by the ADD and DELETE operations */
        Map<Integer, Map<Integer, Entry>> storages  = new HashMap<>();  // NodeId -> ( DataKey -> <Value, Version> )

        /** Storages of all the nodes in the system. Mirrors the state of the system as described by the SET, JOIN, CRASH, etc... operations */
        Map<Integer, Map<Integer, Entry>> simulated = new HashMap<>();  // NodeId -> ( DataKey -> <Value, Version> )

        /** Storage containing all the up-to-date data items */
        Map<Integer, Entry> ideal_storage = new HashMap<>();            // DataKey -> <Value, Version>

        /** List of NodeIDs representing the ring formation in the simulation */
        List<Integer> nodes_in_sim  = new LinkedList<>();

        /** Set of IDs of the nodes that are crashed */
        java.util.Set<Integer> nodes_crashed = new HashSet<Integer>();

        for (Peer p: nodes_in) {
            simulated.put(p.id, new HashMap<>());
            nodes_in_sim.add(p.id);
        }

        try {
            int round = 0;

            File set = new File("seq_cons.txt");
            Scanner scan = new Scanner(set);

            int version, key, node, id;
            String name, value;


            while (scan.hasNextLine()) {
                switch (scan.next()) {
                    case "/////":
                    // Print the ideal storage at the beginning of each round
                    scan.next();
                    scan.next();
                    round = scan.nextInt();
                    System.out.println("---------------------------");
                    System.out.println("STARTING ROUND "+round);
                    break;

                    // ROUND END
                    case "\\\\\\\\\\":
                    // check that the simulated storage and the actual storage contain the same data items for
                    // each node in the ring
                    for (int node_id: nodes_in_sim) {
                        Map<Integer, Entry> sim_storage = simulated.get(node_id);
                        Map<Integer, Entry> actual_storage = storages.get(node_id);
                        if (!check_storage_equality(sim_storage, actual_storage)) {
                            StringBuilder error = new StringBuilder();
                            error.append("Divergence in storages at round ")
                                .append(round).append("\n")
                                .append("Node: ").append(node_id).append("\n")
                                .append("Simulated Storage:\n")
                                .append(storage_to_string(sim_storage)).append("\n")
                                .append("Actual Storage:\n")
                                .append(storage_to_string(actual_storage));
                            return error.toString();
                        }
                    }

                    System.out.println("NODES:");
                    System.out.println(nodes_in_sim.stream().map(node_id->nodes_crashed.contains(node_id)?node_id.toString()+"(crashed)":node_id.toString()).collect(Collectors.joining(" ")));
                    System.out.println("DATA ITEMS:");
                    for (int data_key: ideal_storage.keySet()) {
                        StringBuilder line = new StringBuilder();
                        line.append(data_key).append(": [ ");
                        for (int responsible: getResponsibles(nodes_in_sim, data_key)) {
                            Entry entry = simulated.get(responsible).get(data_key);
                            if (entry == null) continue;
                            line.append(responsible).append("(").append(entry.version).append(") ");
                        }
                        line.append("]");
                        System.out.println(line.toString());
                    }
                    break;

                    // a data item is added when we perform a set, join, leave or recovery operation
                    case "ADD":
                    int add_id = scan.nextInt();
                    int add_key = scan.nextInt();
                    String add_val = scan.next();
                    int add_version = scan.nextInt();
                    storages.computeIfAbsent(add_id, k->new HashMap<>())
                        .put(add_key, new Entry(add_val, add_version));
                    break;

                    // a data item is deleted when we perform a join or recovery operation
                    case "DELETE":
                    int del_id = scan.nextInt();
                    int del_key = scan.nextInt();
                    storages.get(del_id).remove(del_key);
                    break;

                    // SET
                    case "SET":
                    int set_key = scan.nextInt();
                    String set_value = scan.next();
                    int set_version = scan.nextInt();

                    // Check there wasn't a newer version in the system already
                    Entry last_version = ideal_storage.get(set_key);
                    if (last_version!=null && set_version<=last_version.version) {
                        return "Setting version " + set_version +" for key " +set_key+" while version "+last_version.version+" is already present";
                    }
                    ideal_storage.put(set_key, new Entry(set_value, set_version));

                    // Insert the new data
                    List<Integer> resp = getResponsibles(nodes_in_sim, set_key);
                    for (int responsibleId: resp) {
                        if (nodes_crashed.contains(responsibleId)) continue;
                        simulated.get(responsibleId).put(set_key, new Entry(set_value, set_version));
                    }
                    break;

                    case "SET_SUCCESS":
                    name = scan.next();
                    key = scan.nextInt();
                    System.out.println(name + ": SET " + key);
                    break;

                    case "SET_FAIL":
                    name = scan.next();
                    key = scan.nextInt();
                    System.out.println(name + ": SET " + key + " FAIL");
                    break;

                    case "GET":
                    int get_key = scan.nextInt();
                    String get_value = scan.next();
                    int get_version = scan.nextInt();
                    break;

                    case "GET_SUCCESS":
                    name = scan.next();
                    key = scan.nextInt();
                    version = scan.nextInt();
                    System.out.println(name + ": GET " + key + " " +version);
                    break;

                    case "GET_FAIL":
                    name = scan.next();
                    key = scan.nextInt();
                    System.out.println(name+ ": GET " + key + " FAIL");
                    break;

                    case "JOIN":
                    int join_id = scan.nextInt();
                    System.out.println("JOIN " + join_id + " SUCCESS");

                    // Add the node in the ring
                    int index = 0;
                    while (index<nodes_in_sim.size() && join_id>nodes_in_sim.get(index)) index++;
                    nodes_in_sim.add(index, join_id);

                    // Add the data items to the node
                    Map<Integer, Entry> joining_storage = simulated.computeIfAbsent(join_id, jid->new HashMap<>());
                    for (HashMap.Entry<Integer,Entry> e: ideal_storage.entrySet()) {
                        if (isResponsible(nodes_in_sim, join_id, e.getKey())) {
                            joining_storage.put(e.getKey(), e.getValue());
                        }
                    }

                    // Remove the data items from the nodes
                    remove_excess(simulated, nodes_in_sim, nodes_crashed);
                    break;

                    case "JOIN_FAIL":
                    int id_join_fail = scan.nextInt();
                    if (id_join_fail == -1) {
                        System.out.println("JOIN FAIL (no nodes available to join)");
                    }else{
                        System.out.println("JOIN " + id_join_fail + " FAIL");
                    }
                    break;

                    case "LEAVE":
                    int leave_id = scan.nextInt();
                    System.out.println("LEAVE " + leave_id + " SUCCESS");

                    // Remove the node from the ring
                    nodes_in_sim.removeIf(v->v==leave_id);

                    // Move the data items of the node to their new responsible
                    for (HashMap.Entry<Integer, Entry> e: simulated.get(leave_id).entrySet()) {
                        List<Integer> responsibles = getResponsibles(nodes_in_sim, e.getKey());
                        int new_responsible = responsibles.get(responsibles.size()-1);
                        Map<Integer, Entry> new_responsible_storage = simulated.get(new_responsible);
                        Entry old_value = new_responsible_storage.get(e.getKey());
                        if (old_value==null || old_value.version < e.getValue().version) {
                            new_responsible_storage.put(e.getKey(), e.getValue());
                        }
                    }
                    break;

                    case "LEAVE_FAIL":
                    id = scan.nextInt();
                    switch (id) {
                        case -1:
                        System.out.println("LEAVE FAIL (can't have less than N nodes)");
                        break;
                        case -2:
                        System.out.println("LEAVE FAIL (the only active node can't leave)");
                        break;
                        default:
                        System.out.println("LEAVE " + id + " FAIL");
                        break;
                    }
                    break;

                    case "CRASH":
                    id = scan.nextInt();
                    System.out.println("CRASH "+id);
                    nodes_crashed.add(id);
                    break;

                    case "CRASH_FAIL":
                    System.out.println("CRASH FAIL (at least one active node must remain alive)");
                    break;

                    case "RECOVERY":
                    id = scan.nextInt();
                    System.out.println("RECOVERY " + id + "SUCCESS");
                    nodes_crashed.remove(id);

                    // The node gets the new data items.
                    for (int storage_key: ideal_storage.keySet()) {
                        if (!isResponsible(nodes_in_sim, id, storage_key)) continue;
                        Entry latest = null;
                        for (int resp_id: getResponsibles(nodes_in_sim, storage_key)) {
                            if (nodes_crashed.contains(resp_id)) continue;
                            Map<Integer, Entry> s = simulated.get(resp_id);
                            if (s==null) continue;
                            Entry e = s.get(storage_key);
                            if (e==null) continue;
                            if (latest==null || e.version > latest.version) {
                                latest = e;
                            }
                        }
                        simulated.get(id).put(storage_key, latest);
                    }

                    remove_excess(simulated, nodes_in_sim, nodes_crashed);
                    break;

                    case "RECOVERY_FAIL":
                    System.out.println("RECOVERY FAIL (no nodes are crashed)");
                    break;

                    default:
                    scan.nextLine();
                    break;
                };
            }
            scan.close();
            System.out.println("---------------------------");
        }catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        return "No anomalies detected";
    }

    /**
     * It gets the responsible for a data item.
     *
     * @param nodes_in The IDs of the nodes in the ring
     * @param key
     * @return Return a list of the IDs of the nodes responsible for a data key
     */
    private List<Integer> getResponsibles(List<Integer> nodes_in, int key) {
        List<Integer> ret = new LinkedList<>();
        int i = 0;
        while (i<nodes_in.size() && nodes_in.get(i) < key) i++;
        if (i==nodes_in.size()) i = 0;
        for (int j=0; j<N; j++) {
            ret.add(nodes_in.get((i+j)%nodes_in.size()));
        }
        return ret;
    }

    /**
     * It checks if a node is responsible for a data item.
     *
     * @param nodes_in The IDs of the nodes in the ring
     * @param id
     * @param key
     * @return If a node with the given ID is responsible with the key
     */
    private boolean isResponsible(List<Integer> nodes_in, int id, int key) {
        return getResponsibles(nodes_in, key).contains(id);
    }

    /**
     * It checks if the actual storage and the simulated one are equal.
     * @param s1 Storage A
     * @param s2 Storage B
     * @return Whether the two storages are equal
     */
    private boolean check_storage_equality(Map<Integer,Entry> s1, Map<Integer,Entry> s2) {
        if (s1 == null && s2 == null) return true;

        if (s1 == null && s2.isEmpty()) return true;
        if (s2 == null && s1.isEmpty()) return true;

        if (s1 == null || s2 == null) return false;

        return Stream.concat(
            s1.keySet().stream(),
            s2.keySet().stream()
        ).distinct()
        .allMatch(key->Objects.equals(s1.get(key), s2.get(key)));
    }

    /**
     * @param s A storage
     * @return The string representation of this storage
     */
    private String storage_to_string(Map<Integer, Entry> s) {
        if (s==null) {
            return "NULL";
        }
        return s.entrySet().stream()                                                
            .sorted(Comparator.comparingInt(e->e.getKey()))                         
            .map(e->e.getKey()+": "+e.getValue().value+", "+e.getValue().version)   
            .collect(Collectors.joining("\n"));                                     
    }


    /**
     * Removes from each node the data items it is not responsible for.
     * Crashed nodes are not affected.
     *
     * @param storages All the simulated storages
     * @param nodes_in The IDs of the nodes in the ring
     * @param nodes_crashed The IDs of the nodes that have crashed
     */
    private void remove_excess(
        Map<Integer, Map<Integer, Entry>> storages,
        List<Integer> nodes_in,
        java.util.Set<Integer> nodes_crashed) {
        for (int id: nodes_in) {
            if (nodes_crashed.contains(id)) continue;
            Iterator<HashMap.Entry<Integer,Entry>> it = storages.get(id).entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry<Integer,Entry> e = it.next();
                if (!isResponsible(nodes_in, id, e.getKey())) {
                    it.remove();
                }
            }
        }
    }
}
