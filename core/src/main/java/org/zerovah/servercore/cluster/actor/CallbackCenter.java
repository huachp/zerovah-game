package org.zerovah.servercore.cluster.actor;

import akka.actor.ActorRef;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.zerovah.servercore.cluster.ICallback;
import org.zerovah.servercore.cluster.cause.NodeException;
import org.zerovah.servercore.cluster.message.CallbackMessage;
import org.zerovah.servercore.cluster.message.SimpleCallbackMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 集群回调中心
 *
 * @author huachp
 */
class CallbackCenter {

    private static final Logger LOGGER = LogManager.getLogger(CallbackCenter.class);

    private static final CallbackCenter INSTANCE = new CallbackCenter();

    static CallbackCenter get() {
        return INSTANCE;
    }

    /** 回调ID生成器 */
    private final AtomicInteger generator = new AtomicInteger();
    /** {回调ID=回调函数} */
    private final Cache<Integer, CallbackActor> cache = Caffeine.newBuilder()
            .expireAfterWrite(3, TimeUnit.MINUTES)
            .removalListener((key, value, cause) -> {
                if (cause == RemovalCause.EXPIRED) {
                    CallbackActor callbackActor = (CallbackActor) value;
                    if (callbackActor != null) {
                        finalTimeoutCallback((Integer) key, callbackActor);
                    }
                    LOGGER.warn("未处理的回调函数, ID={}, INFO={}", key, callbackActor);
                }
            }).build();


    public CallbackCenter() {
    }

    public int futureCallback(ActorRef workingActor, ICallback callback, Class<?> requestCls) {
        Objects.requireNonNull(callback);
        int callbackId = generator.incrementAndGet() & Integer.MAX_VALUE; // callback id 永远是正数
        if (callbackId == 0) {
            callbackId = generator.incrementAndGet() & Integer.MAX_VALUE; // callback id 不能为0
        }
        CallbackActor callbackActor = new CallbackActor(callback, workingActor, requestCls);
        cache.put(callbackId, callbackActor);
        return callbackId;
    }

    public ICallback takeCallback(Integer callbackId) {
        CallbackActor callbackActor = cache.getIfPresent(callbackId);
        if (callbackActor == null) {
            LOGGER.warn("回调对象不存在, 回调ID:{}", callbackId);
            return null;
        }
        cache.invalidate(callbackId);
        return callbackActor.callback;
    }

    public void processCallback(CallbackMessage message, ActorRef executiveActor) {
        Integer callbackId = message.callbackId();
        CallbackActor callbackActor = cache.getIfPresent(callbackId);
        if (callbackActor == null) {
            LOGGER.warn("回调中心找不到回调Actor处理消息, 回调ID:{}", callbackId);
            return;
        }
        cache.invalidate(callbackId);
        callbackActor.tellInvoke(message, executiveActor);
    }

    // 处理自定义回调超时
    CallbackActor processCustomizedTimeout(int callbackId) {
        CallbackActor callbackActor = cache.getIfPresent(callbackId);
        if (callbackActor != null) {
            callbackActor.reactTimeout();
            return callbackActor;
        }
        return null;
    }

    private void finalTimeoutCallback(Integer key, CallbackActor callbackActor) {
        if (callbackActor.isTimeout()) {
            LOGGER.info("清理超时的回调函数, 回调ID={}", key); return;
        }
        ActorRef execActorRef = callbackActor.actorRef;
        String requestClsName = callbackActor.requestClass.getSimpleName();
        NodeException nodeEx = NodeException.remoteTimeout(requestClsName);
        if (execActorRef != null) {
            SimpleCallbackMsg msg = SimpleCallbackMsg.create(nodeEx);
            msg.linkCallback(key);
            msg.readyOnCall(callbackActor.callback, callbackActor.timeout);
            execActorRef.tell(msg, ActorRef.noSender());
            LOGGER.warn("未处理的回调函数, 回调ID={}, 发起消息={}", key, callbackActor.requestClass);
        } else {
            LOGGER.warn("注意: 回调逻辑执行Actor引用为null, 远程消息超时不处理, 回调ID:{}, 超时消息:{}",
                    key, requestClsName);
        }
    }


    static class CallbackActor {

        private final ICallback callback;
        private ActorRef actorRef;
        private Class<?> requestClass;
        private AtomicInteger timeout; // -1正常执行完成, 0未超时初始状态, 1已超时, 2超时执行完成

        CallbackActor(ICallback callback, ActorRef actorRef, Class<?> requestCls) {
            this.actorRef = actorRef;
            this.callback = callback;
            this.requestClass = requestCls;
            this.timeout = new AtomicInteger();
        }

        // 成功回调处理
        void tellInvoke(CallbackMessage message, ActorRef defaultActor) {
            message.readyOnCall(callback, timeout);
            if (actorRef != null) {
                actorRef.tell(message, ActorRef.noSender());
            } else {
                defaultActor.tell(message, ActorRef.noSender());
            }
            requestClass = null;
        }

        ICallback callbackObj() {
            return callback;
        }

        boolean isTimeout() {
            return timeout.get() > 0;
        }

        void reactTimeout() {
            synchronized (callback) {
                if (timeout.get() == 0) {
                    timeout.getAndSet(1);
                }
            }
        }

        // 超时回调处理
        void executeTimeoutCallback(Object timtoutData) {
            callback.onCall(timtoutData);
            timeout.getAndSet(2);
        }

        @Override
        public String toString() {
            return '{' +
                    "message=" + requestClass.getSimpleName() + ',' +
                    " localActor=" + (actorRef == null ? null : actorRef.path().name()) +
                    '}';
        }
    }

}
