package org.zerovah.servercore.cluster.actor;

import akka.actor.ActorRef;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.zerovah.servercore.cluster.message.HeartbeartMsg;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 故障检测记录
 *
 * @author huachp
 */
public class FaultDetectionRecord {

    private static final int FAILURE_COUNT = 5;
    private static final int HEART_BEAT_TIMEOUTS = 5;

    private int connectionFailures;
    private int heartbeatTimeouts;
    private boolean assumedFailure; // 假定节点故障
    private boolean judgedFailure; // 判定节点故障

    private CountDownLatch pendingHandledCallbacks; // 待处理回调数

    private boolean usable = true;

    FaultDetectionRecord() {
    }

    public int getConnectionFailures() {
        return connectionFailures;
    }

    public int getHeartbeatTimeouts() {
        return heartbeatTimeouts;
    }

    public boolean isAssumedFailure() {
        return assumedFailure;
    }

    public boolean isJudgedFailure() {
        return judgedFailure;
    }

    public boolean isUsable() {
        return usable;
    }

    public void judgeNodeFailure() {
        judgedFailure = true;
    }

    public boolean isInitiateFaultElection() {
        return assumedFailure && !judgedFailure;
    }

    public void addConnectionFailures() {
        connectionFailures ++;
        if (connectionFailures >= FAILURE_COUNT) {
            assumedFailure = true;
        }
    }

    public void addHeartbeatTimeouts() {
        heartbeatTimeouts ++;
    }

    public boolean isHeartbeatTimeoutsExceededLimit() {
        if (heartbeatTimeouts >= HEART_BEAT_TIMEOUTS
                && heartbeatTimeouts < HEART_BEAT_TIMEOUTS * 2) {
            assumedFailure = true;
            return true;
        }
        return false;
    }

    public boolean isAppearedProblemWithHeartbeat() {
        return heartbeatTimeouts >= HEART_BEAT_TIMEOUTS * 3;
    }

    public void resetConnectionFailures() {
        connectionFailures = 0;
    }

    public void resetHeartbeatTimeouts() {
        heartbeatTimeouts = 0;
    }

    public void resetFailure() {
        assumedFailure = false;
    }

    public void handledCallbacksCountDown() {
        pendingHandledCallbacks.countDown();
    }

    public void readyPendingHandledCallbacks(int callbacks) {
        pendingHandledCallbacks = new CountDownLatch(callbacks);
    }

    public boolean isHandledAllFaultCallbacks() {
        if (pendingHandledCallbacks == null) {
            return false;
        }
        return pendingHandledCallbacks.getCount() == 0;
    }

    public boolean isHandlingFault() {
        return pendingHandledCallbacks != null;
    }

    public void resetDetection() {
        resetConnectionFailures();
        resetHeartbeatTimeouts();
        resetFailure();
//        judgedFailure = false;
//        pendingHandledCallbacks = null;
    }

    public void markUsable(boolean flag) {
        usable = flag;
    }


    static class HeartbeatCommunication {

        private final AtomicInteger generator = new AtomicInteger();

        private final Cache<Short, HeartbeatPending> pendingTasks = Caffeine.newBuilder()
                .expireAfterWrite(5, TimeUnit.SECONDS)
                .removalListener((key, value, cause) -> {
                    if (cause == RemovalCause.EXPIRED) {
                        HeartbeatPending pendingObj = (HeartbeatPending) value;
                        if (pendingObj != null) {
                            pendingObj.respondActorTimeout();
                        }
                    }
                }).build();


        void addPendingTask(ActorRef actorRef, HeartbeartMsg msg) {
            int id = generator.incrementAndGet();
            short heartbeatId = msg.injectId(id).getHeartbeatId();
            pendingTasks.put(heartbeatId, new HeartbeatPending(actorRef, msg));
        }

        void processPendingTask(HeartbeartMsg responseHeartbeat) {
            short heartbeatId = responseHeartbeat.getHeartbeatId();
            HeartbeatPending pendingObj = pendingTasks.getIfPresent(heartbeatId);
            if (pendingObj != null) {
                pendingObj.respondActorSuccess(responseHeartbeat);
                pendingTasks.invalidate(heartbeatId);
            }
        }

    }

    static class HeartbeatPending {

        private ActorRef actorRef; // 心跳执行Actor引用
        private HeartbeartMsg message; // 心跳消息

        HeartbeatPending(ActorRef actorRef, HeartbeartMsg msg) {
            this.actorRef = actorRef;
            this.message = msg;
        }

        void respondActorSuccess(HeartbeartMsg responseHeartbeat) {
            actorRef.tell(responseHeartbeat, ActorRef.noSender());
        }

        void respondActorTimeout() {
            actorRef.tell(message.timeout(), ActorRef.noSender());
        }
    }

    public static void prepareHeartbeat(ActorRef actorRef, HeartbeartMsg msg) {
        HEARTBEAT.addPendingTask(actorRef, msg);
    }

    public static void processHeartbeatResponse(HeartbeartMsg msg) {
        HEARTBEAT.processPendingTask(msg);
    }


    static final HeartbeatCommunication HEARTBEAT = new HeartbeatCommunication();

}
