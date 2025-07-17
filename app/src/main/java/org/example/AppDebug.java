package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.example.msg.Set;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

import static org.example.App.*;

public class AppDebug {

    public static final int SET = 4;
    public static final int CRASH = 0;

    public ActorSystem system;
    public List<ActorRef> clients;
    public List<ActorRef> nodes;

    public AppDebug(String name) {
        this.system = ActorSystem.create(name);
        this.clients = new ArrayList<>();
        this.nodes = new ArrayList<>();
    }

    public class ErrorDebug{
        String error;
        boolean noError;

        public ErrorDebug(String error, boolean noError) {
            this.error = error;
            this.noError = noError;
        }
    }

    public void addClients(){
        for (int i = 0; i<C; i++){
            this.clients.add(this.system.actorOf(Client.props(generateRandomString(4))));
        }
    }

    public void addNodes(){
        for (int i=0; i<START; i++) {
            this.nodes.add(this.system.actorOf(Node.props((i+1)*10)));
        }

        for (int i=0; i<START; i++) {
            ActorRef n = this.nodes.get(i);
            for (ActorRef node: this.nodes) {
                node.tell(new Node.DebugAddNodeMsg(n, (i+1)*10), n);
            }
        }
    }

    public ErrorDebug setFixedTest(){

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
        int bound = (START+1)*10;
        for (int i = 0; i<SET; i++){
            int key = rng.nextInt(bound);
            String value = generateRandomString(3);
            this.nodes.get(rng.nextInt(nodes.size())).tell(new Set.InitiateMsg(key, value), this.clients.get(0));
        }

        try { Thread.sleep(T_END); } catch (InterruptedException e) {e.printStackTrace(); }

        try {
            console.println(">> Press Enter to End <<");
            System.in.read();
        }catch (Exception e) {}
        this.system.terminate();
        System.setOut(console);

        // read the file and check it
        List<Integer> items = new ArrayList<>();
        Map<Integer,List<Integer>> storage = new HashMap<>();

        for (int i = 0; i<START; i++){
            storage.put(((i+1)*10),new ArrayList<>());
        }

        int fails = 0;

        try {
            File set = new File("set.txt");
            Scanner scan = new Scanner(set);
            while (scan.hasNextLine()) {
                if (scan.hasNext("W")){
                    scan.next();
                    int k = scan.nextInt();
                    scan.next();
                    int item = scan.nextInt();
                    if (!items.contains(item)){
                        items.add(item);
                    }
                    storage.get(k).add(item);
                } else{
                    String msg = scan.nextLine();
                    if (msg.contains("Fail")){
                        fails++;
                    }
                }
            }
            scan.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // check number of data items
        if (items.size()!=SET-fails){
            return new ErrorDebug("Number of data items uncorrected",false);
        }

        // check data items assignment
        for (int e : items){
            int index = 0;
            while (index < storage.size() && ((index+1)*10)<e){
                index++;
            }
            if (index==storage.size()) { index = 0; }
            for (int i = 0; i<N; i++){
                int sign = 1;
                if (!storage.get((((i+index)%START)+1)*10).contains(e)){
                    return new ErrorDebug("Node "+((((i+index)%START)+1)*10)+" doesn't contain data item "+e,false);
                }
            }
        }

        return new ErrorDebug("Set test is correct",true);

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
