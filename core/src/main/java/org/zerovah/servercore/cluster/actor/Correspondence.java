package org.zerovah.servercore.cluster.actor;

import akka.actor.Actor;
import akka.actor.ActorRef;

/**
 * 通信Actor统一API接口
 *
 * @author huachp
 */
public interface Correspondence {

    boolean isFaultyNode();

    boolean actuallyFaultyNode();

    boolean isVoteTargetNodeFault();

    int connectedNodeType();

    int connectedNodeId();

    String connectedIp();

    boolean isNodeUsable();

    void markNodeUsable(boolean usable);

    ActorRef newChildActorRef();

    ActorRef childActor();

    Actor transform();

}
