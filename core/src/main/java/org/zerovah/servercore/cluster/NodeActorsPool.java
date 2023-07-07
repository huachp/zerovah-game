package org.zerovah.servercore.cluster;

import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.actor.Correspondence;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 集群节点actor池
 *
 * @author huachp
 */
public class NodeActorsPool {

    /** 池锁 */
    private Lock poolLock = new ReentrantLock();
    /** 节点Actors */
    private ConcurrentLinkedDeque<Correspondence> clusterActors = new ConcurrentLinkedDeque<>();
    /** 节点广播ActorRef列表 */
    private MixedNodeActor broadcastActor = MixedNodeActor.create();

    public NodeActorsPool() {

    }

    public void addActor(Correspondence correspondenceActor) {
        clusterActors.add(correspondenceActor);
        ActorRef actorRef = correspondenceActor.childActor();
        broadcastActor.addActorRef(correspondenceActor.connectedNodeId(), actorRef);
    }

    public Correspondence getActor(RouteStrategy<Correspondence> strategy, int key) {
        return strategy.route(clusterActors, key);
    }

    public void removeActor(Correspondence actor) {
        toLock();
        try {
            Iterator<Correspondence> it = clusterActors.iterator();
            for ( ; it.hasNext(); ) {
                Correspondence element = it.next();
                if (element == actor) {
                    it.remove();
                    broadcastActor.removeActorRef(element.connectedNodeId());
                    break;
                }
            }
        } finally {
            unLock();
        }
    }

    public MixedNodeActor getBroadcastActor() {
        return broadcastActor;
    }

    public int normalNodesCount() {
        Iterator<Correspondence> it = clusterActors.iterator();
        int count = 0;
        for (; it.hasNext(); ) {
            Correspondence element = it.next();
            if (!element.isFaultyNode()) {
                count ++;
            }
        }
        return count;
    }

    public int getMinimumNodeId() {
        return broadcastActor.getFirstNodeId();
    }

    Collection<Correspondence> actors() {
        return clusterActors;
    }

    void toLock() {
        poolLock.lock();
    }

    void unLock() {
        poolLock.unlock();
    }


    public static NodeActorsPool create() {
        return new NodeActorsPool();
    }

}
