package org.zerovah.servercore.cluster.actor;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.message.HeartbeartMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;

/**
 * (中继节点->中继节点)中继节点互连服务端Actor
 *
 * @author huachp
 */
public class RelayServerActor extends AbstractActorWithTimers {

    private static final Logger LOGGER = LogManager.getLogger(RelayServerActor.class);

    private final Receive receive = ReceiveBuilder.create()
            .match(Consumer.class, this::forward)
            .match(HeartbeartMsg.class, this::heartbeat)
            .matchAny(this::doOtherOperation)
            .build();

    public RelayServerActor() {
    }

    @Override
    public Receive createReceive() {
        return receive;
    }

    private void forward(Consumer<ActorRef> message) {
        try {
            message.accept(self());
        } catch (Exception e) {
            LOGGER.error("转发逻辑异常", e);
        }
    }

    private void heartbeat(HeartbeartMsg heartbeartMsg) {
        try {
            if (heartbeartMsg.isRequest()) {
                sender().tell(heartbeartMsg, ActorRef.noSender());
            } else if (heartbeartMsg.isResponse()) {
                FaultDetectionRecord.processHeartbeatResponse(heartbeartMsg);
            } else {
                LOGGER.warn("中继节点收到不合法的心跳状态");
            }
        } catch (Exception e) {
            LOGGER.error("心跳消息出现异常", e);
        }
    }

    private void doOtherOperation(Object message) {

    }


    @Override
    public void preStart() throws Exception {
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("中继服务端Actor故障重启-> {}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
        LOGGER.info("中继服务端Actor停止运行-> {}", getSelf());
    }

}
