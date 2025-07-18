package org.example.shared;

import akka.actor.ActorRef;

public class NamedClient {
    public final String name;
    public final ActorRef ref;

    public NamedClient(String name, ActorRef ref) {
        this.name = name;
        this.ref = ref;
    }
}
