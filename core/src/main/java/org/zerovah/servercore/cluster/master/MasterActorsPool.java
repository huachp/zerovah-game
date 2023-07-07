package org.zerovah.servercore.cluster.master;

import akka.actor.Actor;
import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.ICallback;
import org.zerovah.servercore.cluster.INodeFuture;
import org.zerovah.servercore.cluster.message.MasterMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Master Actor连接池
 *
 * @author huachp
 */
public class MasterActorsPool {

    private static final Logger LOGGER = LogManager.getLogger(MasterActorsPool.class);

    /** Master Actors */
    private NavigableMap<Integer, MasterStatus> masterActors = new ConcurrentSkipListMap<>();

    public MasterActorsPool() {
    }

    public void addMaster(Actor actor, int masterId, RaftState state) {
        if (state == RaftState.LEADER) {
            MasterStatus status = new MasterStatus(actor);
            status.masterId = masterId;
            status.state = state;
            masterActors.put(masterId, status);
        } else {
            LOGGER.info("Master[{}]不是RaftState#{}", masterId, state.name());
        }
    }

    private ActorRef getMasterActor() {
        Entry<Integer, MasterStatus> first = masterActors.firstEntry();
        Objects.requireNonNull(first);
        return first.getValue().actor.self();
    }

    public void tell(MasterMessage message) {
        getMasterActor().tell(message, ActorRef.noSender());
    }

    public INodeFuture addReconnectionEvent(ICallback callback) {
        MasterReconnectFuture future = new MasterReconnectFuture();
        getMasterActor().tell(future, ActorRef.noSender());
        return future;
    }


    static class MasterStatus {

        int masterId;
        Actor actor;
        RaftState state;

        MasterStatus(Actor actor) {
            this.actor = actor;
        }

    }

    public static class MasterReconnectFuture implements INodeFuture {

        private volatile boolean success;

        private Queue<ICallback> callbacks = new ConcurrentLinkedQueue<>();

        private MasterReconnectFuture() {
        }

        public void succeedAndExecute() {
            synchronized (this) {
                this.success = true;
                for ( ; callbacks.peek() != null; ) {
                    ICallback callback = callbacks.poll();
                    callback.onCall(success);
                }
            }
        }

        @Override
        public INodeFuture addConnectedListener(int nodeType, ICallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public INodeFuture addListener(ICallback callback) {
            callbacks.offer(callback);
            return this;
        }
    }

}
