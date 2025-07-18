package org.example.shared;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import akka.japi.Pair;

public class Graph {
    public Set<String> nodes;
    public Map<String, Set<String>> edges;

    public Graph() {
        this.nodes = new HashSet<>();
        this.edges = new HashMap<>();
    }

    public boolean addNode(String node) {
        edges.put(node, new HashSet<>());
        return this.nodes.add(node);
    }

    public void addEdge(String n1, String n2) {
        this.edges.get(n1).add(n2);
    }

    public List<String> check_topological_ordering() {
        List<String> L = new LinkedList<>();
        Set<String> S = new HashSet<>();
        S.addAll(this.nodes);
        for (Entry<String, Set<String>> e: this.edges.entrySet()) {
            S.removeAll(e.getValue());
        }

        while (!S.isEmpty()) {
            String n = S.iterator().next();
            S.remove(n);

            L.add(n);
            for (String m: edges.get(n)) {
                edges.get(n).remove(m);
                if (!has_incomming(m)) {
                    S.add(m);
                }
            }
        }

        boolean ok = true;
        for (Entry<String, Set<String>> e: this.edges.entrySet()) {
            if (!e.getValue().isEmpty()) {
                ok = false;
            }
            for (String n2: e.getValue()) {
                System.out.println(e.getKey()+" -> " + n2);
            }
        }
        if (!ok) return null;

        return L;
    }

    private boolean has_incomming(String n) {
        for (String n2: this.nodes) {
            if (edges.get(n2).contains(n)) return true;
        }

        return false;
    }
}
