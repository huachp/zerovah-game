package org.zerovah.servercore.cluster.actor;

import akka.actor.Actor;
import akka.actor.ActorRef;
import org.zerovah.servercore.cluster.message.AbstractCallbackMsg;
import org.zerovah.servercore.cluster.message.CallbackMessage;
import org.zerovah.servercore.cluster.message.SimpleCallbackMsg;

/**
 * 集群对端Actor上下文
 *
 * @author huachp
 */
public class PeersActorContext {

    /** 回调ID */
    private int callbackId;
    /** 对端发送者的actorId */
    private long senderActorId;
    /** 本地处理Actor */
    private ActorRef self;
    /** 对端网络Actor */
    private ActorRef sender;
    /** 是否已回复 */
    private boolean replied;

    public PeersActorContext() {
    }

    public ActorRef getSelf() {
        return self;
    }

    public void reply(Object data) {
        if (callbackId <= 0) { // >0 正常回调, <0 中继回调
            return;
        }
        CallbackMessage callbackMsg = SimpleCallbackMsg.create(data)
                .senderActor(senderActorId)
                .linkCallback(callbackId);
        sender.tell(callbackMsg, ActorRef.noSender());
        replied = true;
    }

    public void reply(CallbackMessage message) {
        if (callbackId <= 0) {
            return;
        }
        if (message instanceof AbstractCallbackMsg) {
            ((AbstractCallbackMsg) message).senderActor(senderActorId);
        }
        sender.tell(message.linkCallback(callbackId), ActorRef.noSender());
        replied = true;
    }

    public ClusterActor changeSender() {
        return new ClusterActor.OneWayClusterActor(sender);
    }

    boolean isRecycled() {
        return replied;
    }

    public long getSenderActorId() {
        return senderActorId;
    }

    void clear() {
        this.callbackId = -1;
        this.senderActorId = 0;
        this.self = null;
        this.sender = null;
        this.replied = false;
    }

    void refresh(int id, long clientActorId, Actor baseActor) {
        this.callbackId = id;
        this.senderActorId = clientActorId;
        this.self = baseActor.self();
        this.sender = baseActor.sender();
    }


//    public static PeersActorContext create(int id, Actor baseActor) {
//        PeersActorContext ctx = new PeersActorContext();
//        ctx.self = baseActor.self();
//        ctx.sender = baseActor.sender();
//        ctx.callbackId = id;
//        return ctx;
//    }

}
