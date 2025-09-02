package org.example.shared;

import akka.actor.ActorRef;

/**
 * This class represents a Client operating in the system.
 */
public class NamedClient {
    /** Name of the client. */
    public final String name;
    /** ActorRef of the client. */
    public final ActorRef ref;

    public NamedClient(String name, ActorRef ref) {
        this.name = name;
        this.ref = ref;
    }
}
