package org.example.msg;

import akka.actor.ActorRef;

import java.io.Serializable;

public class Debug {

    public static class AddNodeMsg implements Serializable {
        public final ActorRef ref;
        public final int id;
        public AddNodeMsg(ActorRef ref, int id) {
            this.ref = ref;
            this.id = id;
        }
    }

    public static class AddClientMsg implements Serializable{
        public final ActorRef ref;
        public final String name;
        public AddClientMsg(ActorRef ref, String name ) {
            this.ref = ref;
            this.name = name;
        }
    }

    public static class StartRoundMsg implements Serializable{ }

    public static class IncreaseOngoingMsg implements Serializable{
        public final ActorRef responsible;

        public IncreaseOngoingMsg(ActorRef responsible) {
            this.responsible = responsible;
        }
    }

    public static class DecreaseOngoingMsg implements Serializable{ }

    public static class AnnounceCoordinator implements Serializable{
        public final ActorRef coordinator;

        public AnnounceCoordinator(ActorRef coordinator) {
            this.coordinator = coordinator;
        }
    }




}
