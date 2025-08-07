package org.example.shared;

import org.example.Node.Peer;

import akka.actor.ActorRef;

public class Operation {
    public static enum Ops {
        SET,
        GET,
        JOIN,
        LEAVE,
        RECOVER,
        CRASH
    }

    public final Ops op;
    public final boolean success;
    public final Peer peer;

    public Operation(Ops op, boolean success, Peer peer) {
        this.op = op;
        this.success = success;
        this.peer = peer;
    }
}
