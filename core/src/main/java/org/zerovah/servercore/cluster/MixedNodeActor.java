package org.zerovah.servercore.cluster;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import org.zerovah.servercore.cluster.message.RemoteMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 混合节点(同一类型)集群Actor, 主要做广播业务
 *
 * @author huachp
 */
public class MixedNodeActor {
    
    private static final Logger LOGGER = LogManager.getLogger(MixedNodeActor.class);

    private int localNodeId;

    private CopyOnWriteArrayList<RemoteActorRef> broadcastActors = new CopyOnWriteArrayList<>();

    // 操作是串行的
    synchronized void addActorRef(int nodeId, ActorRef clientActor) {
        RemoteActorRef remoteActorRef = new RemoteActorRef(nodeId, clientActor);
        if (broadcastActors.isEmpty()) {
            broadcastActors.add(remoteActorRef);
            return;
        }
        int listIdx = Collections.binarySearch(broadcastActors, remoteActorRef);
        if (listIdx >= 0) {
            RemoteActorRef existActor = broadcastActors.get(listIdx);
            existActor.actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
            LOGGER.warn("注意:存在相同节点ID[{}]的远程Actor({}),覆盖", nodeId, existActor);
            broadcastActors.set(listIdx, remoteActorRef);
        } else {
            listIdx = Math.abs(listIdx) - 1;
            broadcastActors.add(listIdx, remoteActorRef);
        }
    }

    void removeActorRef(int nodeId) {
        broadcastActors.removeIf(remoteActorRef -> remoteActorRef.nodeId == nodeId);
    }

    /**
     * 给关注的所有节点发送远程消息, 不等待回应, 不带callback
     *
     * @param message 远程消息
     */
    public void tellAll(RemoteMessage message) {
        if (broadcastActors.isEmpty()) {
            return;
        }
        message.sourceNodeId(localNodeId);
        for (RemoteActorRef remoteActorRef : broadcastActors) {
            remoteActorRef.actorRef.tell(message, ActorRef.noSender());
        }
    }

    public int getFirstNodeId() {
        return broadcastActors.isEmpty() ? -1 : broadcastActors.get(0).nodeId;
    }

    List<RemoteActorRef> actorRefs() {
        return broadcastActors;
    }

    MixedNodeActor localNode(int localNodeId) {
        if (this.localNodeId <= 0) {
            this.localNodeId = localNodeId;
        }
        return this;
    }

    @Override
    public String toString() {
        if (this == DEFAULT) {
            return "CompositeClusterActor[" + DEFAULT + "]@" + Integer.toHexString(hashCode()) ;
        } else {
            return super.toString();
        }
    }


    static MixedNodeActor create() {
        return new MixedNodeActor();
    }

    public static final MixedNodeActor DEFAULT = MixedNodeActor.create();

    static class RemoteActorRef implements Comparable<RemoteActorRef> {

        int nodeId;
        ActorRef actorRef;

        private RemoteActorRef(int nodeId, ActorRef actorRef) {
            this.nodeId = nodeId;
            this.actorRef = actorRef;
        }

        @Override
        public int compareTo(RemoteActorRef obj) {
            return this.nodeId - obj.nodeId;
        }
    }

}
