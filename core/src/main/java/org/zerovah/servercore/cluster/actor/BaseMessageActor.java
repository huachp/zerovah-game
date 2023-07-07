package org.zerovah.servercore.cluster.actor;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import org.zerovah.servercore.cluster.ProcessingCenter;
import org.zerovah.servercore.cluster.cause.NodeException;
import org.zerovah.servercore.cluster.message.*;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 基础消息Actor, 绑定{@code ProcessingCenter}
 * <p>
 * 注意：
 *    子类继承{@code BaseMessageActor}必须覆盖构造方法, 且构造方法第一个参数必须是{@code ProcessingCenter}
 * </p>
 *
 * @author huachp
 */
public class BaseMessageActor extends AbstractActorWithTimers {

    public static final AtomicInteger ACTOR_COUNT_RECORD = new AtomicInteger();

    private static final Logger LOGGER = LogManager.getLogger(BaseMessageActor.class);

    private static final TimeoutTicker TIMEOUT_TICKER = new TimeoutTicker();

    private static final FastThreadLocal<LinkedList<PeersActorContext>> PEERS_ACTOR_POOL
            = new FastThreadLocal<LinkedList<PeersActorContext>>() {

        @Override
        protected LinkedList<PeersActorContext> initialValue() throws Exception {
            return new LinkedList<>();
        }

    };

    static volatile ActorRef timeoutActor;

    static synchronized void prepareMonitorActor(ActorSystem actorSys) {
        if (timeoutActor == null) {
            String actorName = "callback-timeout-detector";
            Props props = BaseMessageActor.props(null);
            timeoutActor = actorSys.actorOf(props, actorName);
        }
    }

    static void timeoutMonitoring(CallbackTimeoutCDMsg message) {
        timeoutActor.tell(message, ActorRef.noSender());
    }


    /** 消息处理中心 */
    private ProcessingCenter processingCenter;
    /** 集群Actor唯一标识 */
    private Long clusterActorId = 0L;
    /** 超时检测消息缓存 */
    private Map<Integer, CallbackTimeoutCDMsg> timeoutRelationMap = new HashMap<>();
    /** 超时队列 */
    private LinkedList<CallbackTimeoutCDMsg> timeoutCallbackList = new LinkedList<>();

    private final Receive receive = ReceiveBuilder.create()
            .match(RemoteMessage.class, this::process)
            .match(CallbackMessage.class, this::callback)
            .match(CallbackTimeoutCDMsg.class, this::startCallbackTimeoutTicker)
            .match(BindingActorMsg.class, this::bindingActorId)
            .match(HeartbeartMsg.class, this::heartbeat)
            .match(RemoveItemInTimeoutQueue.class, this::removeFromTimeoutQueue)
            .match(TimeoutTicker.class, this::timeoutDetection)
            .matchAny(this::processOtherMsg)
            .build();

    public BaseMessageActor(ProcessingCenter processingCenter) {
        this.processingCenter = processingCenter;
    }

    public static Props props(ProcessingCenter processingCenter) {
        return Props.create(BaseMessageActor.class, processingCenter);
    }

    protected PeersActorContext getPeersCtxFromPool() {
        LinkedList<PeersActorContext> ctxPool = PEERS_ACTOR_POOL.get();
        PeersActorContext peersCtx = ctxPool.poll();
        if (peersCtx == null) {
            peersCtx = new PeersActorContext();
        }
        return peersCtx;
    }

    protected void recyclePeersCtx(PeersActorContext peersCtx) {
        LinkedList<PeersActorContext> ctxPool = PEERS_ACTOR_POOL.get();
        ctxPool.addLast(peersCtx);
    }

    @Override
    public Receive createReceive() {
        return receive;
    }

    private void process(RemoteMessage remoteMessage) {
        PeersActorContext peersCtx = getPeersCtxFromPool();
        int callbackId = remoteMessage.getId();
        long actorId = remoteMessage.getActorId();
        try {
            peersCtx.refresh(callbackId, actorId, this);
            Object returnObj = processingCenter.fastInvoke(remoteMessage, peersCtx);
            if (returnObj instanceof ActorRef) {
                ActorRef businessActor = (ActorRef) returnObj;
                long clusterActorId = remoteMessage.getActorId();
                businessActor.tell(BindingActorMsg.create(clusterActorId), ActorRef.noSender());
                processingCenter.getDistributor().bindingActor(clusterActorId, businessActor);
            }
        } catch (Exception e) {
            LOGGER.error("执行集群逻辑消息发生异常, {}", remoteMessage, e);
            String exceptionCls = e.getClass().getSimpleName();
            NodeException nodeEx = NodeException.remoteFailure(exceptionCls + " ->" + e.getMessage());
            peersCtx.reply(nodeEx);
        } finally {
            if (peersCtx.isRecycled()) {
                peersCtx.clear();
                recyclePeersCtx(peersCtx);
            }
        }
    }

    private void callback(CallbackMessage callbackMsg) {
        try {
            int callbackId = callbackMsg.callbackId();
            // 优先通知超时队列消息已正常处理
            removeFromTimeoutQueue(callbackId);
            timeoutActor.tell(new RemoveItemInTimeoutQueue(callbackId), self());

            boolean success = callbackMsg.callback();
            if (!success) {
                LOGGER.warn("回调ID[{}]没有回调函数, 请检查逻辑", callbackId);
            }
        } catch (Exception e) {
            LOGGER.error("执行节点回调消息发生异常, {}", callbackMsg, e);
        }
    }

    private void removeFromTimeoutQueue(int callbackId) {
        CallbackTimeoutCDMsg timeoutObj = timeoutRelationMap.remove(callbackId); // 正常回调移除超时检测队列
        if (timeoutObj != null) {
            Comparator<CallbackTimeoutCDMsg> c = CallbackTimeoutCDMsg.CD_COMPARATOR;
            int index = Collections.binarySearch(timeoutCallbackList, timeoutObj, c);
            if (index >= 0) {
                timeoutCallbackList.remove(index);
            } else {
                LOGGER.warn("回调ID[{}]计时消息在超时队列不存在, 请注意", callbackId);
            }
        }
    }

    private void removeFromTimeoutQueue(RemoveItemInTimeoutQueue message) {
        removeFromTimeoutQueue(message.callbackId);
    }

    private void heartbeat(HeartbeartMsg heartbeartMsg) {
        try {
            if (heartbeartMsg.isRequest()) {
                sender().tell(heartbeartMsg, ActorRef.noSender());
                LOGGER.trace("收到节点客户端心跳消息");
            } else if (heartbeartMsg.isResponse()) {
                FaultDetectionRecord.processHeartbeatResponse(heartbeartMsg);
            } else {
                LOGGER.warn("收到不合法的心跳状态消息");
            }
        } catch (Exception e) {
            LOGGER.error("心跳消息出现异常", e);
        }
    }

    private void bindingActorId(BindingActorMsg message) {
        long clusterActorId = message.getClusterActorId();
        if (this.clusterActorId > 0L) {
            String myName = self().path().name();
            LOGGER.warn("注意: Actor[{}]原已绑定集群ActorId[{}], 现重复绑定集群ActorId[{}], 请检查逻辑",
                    myName, this.clusterActorId, clusterActorId);
        }
        this.clusterActorId = clusterActorId;
    }

    private void startCallbackTimeoutTicker(CallbackTimeoutCDMsg message) {
        if (timeoutRelationMap.isEmpty()) {
            getTimers().startPeriodicTimer(TimeoutTicker.class, TIMEOUT_TICKER, Duration.ofSeconds(5L));
        }
        timeoutRelationMap.put(message.getCallbackId(), message);

        CallbackTimeoutCDMsg last = timeoutCallbackList.peekLast(); // last one
        if (last == null || message.getEndTime() > last.getEndTime()) {
            timeoutCallbackList.add(message);
            return;
        }
        Comparator<CallbackTimeoutCDMsg> c = CallbackTimeoutCDMsg.CD_COMPARATOR;
        int index = Collections.binarySearch(timeoutCallbackList, message, c);
        if (index < 0) {
            index = -(index + 1);
            timeoutCallbackList.add(index, message);
        } else {
            LOGGER.warn("远程消息超时队列已有相同的插入对象, 回调ID={}, 请检查逻辑", message.getCallbackId());
        }
    }

    private void timeoutDetection(TimeoutTicker ticker) {
        while (timeoutCallbackList.size() > 0) {
            try {
                CallbackTimeoutCDMsg timeoutMsg = timeoutCallbackList.peek(); // 象征性取出头部
                if (timeoutMsg.isTimeout()) {
                    int callbackId = timeoutMsg.getCallbackId();
                    timeoutCallbackList.poll(); // 取出头部
                    timeoutRelationMap.remove(callbackId); // 干掉映射关系缓存

                    CallbackCenter cbCenter = CallbackCenter.get();
                    CallbackCenter.CallbackActor callbackActor = cbCenter.processCustomizedTimeout(callbackId);
                    if (callbackActor == null) {
                        LOGGER.warn("自定义超时回调处理找不到回调逻辑, 超时消息:{}", timeoutMsg);
                        return;
                    }
                    synchronized (callbackActor.callbackObj()) { // 锁回调对象
                        if (!callbackActor.isTimeout()) {
                            return;
                        }
                        int timeoutSeconds = timeoutMsg.getTimeoutSeconds();
                        String detail = "customized timeout seconds: " + timeoutSeconds;
                        NodeException nodeException = new NodeException(detail);
                        callbackActor.executeTimeoutCallback(nodeException); // 自定义超时N秒处理
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
                LOGGER.error("处理超时回调出现异常", e);
            }
        }

        if (timeoutRelationMap.isEmpty()) {
            getTimers().cancel(TimeoutTicker.class); // 移除定时器
        }
    }

    protected void processOtherMsg(Object message) {

    }

    @Override
    public void preStart() throws Exception {
//        LOGGER.debug("消息处理Actor初始化启动, ActorRef={}", getSelf());
        ACTOR_COUNT_RECORD.incrementAndGet(); // 服务端Actor对象计数
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        LOGGER.error("消息处理Actor故障重启, ActorRef={}, Reason:{}", getSelf(), reason);
    }

    @Override
    public void postStop() throws Exception {
//        LOGGER.info("消息处理Actor停止运行, ActorRef={}", getSelf());
        if (clusterActorId > 0) {
            processingCenter.getDistributor().unbinding(clusterActorId, getSelf());
        }
        ACTOR_COUNT_RECORD.decrementAndGet();
    }


    private static class TimeoutTicker {

    }

    private static class RemoveItemInTimeoutQueue {

        private int callbackId;

        RemoveItemInTimeoutQueue(int callbackId) {
            this.callbackId = callbackId;
        }
    }

}
