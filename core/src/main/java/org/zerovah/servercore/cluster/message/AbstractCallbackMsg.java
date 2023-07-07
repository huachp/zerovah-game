package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.ICallback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 抽象回调消息
 *
 * @author huachp
 */
public abstract class AbstractCallbackMsg implements CallbackMessage {

    private static final Object EMPTY = new Object();

    protected int id = -1;

    private Object returnData;

    private long actorId;

    protected transient ICallback callbackObj;
    protected transient AtomicInteger timeout;

    @Override
    public int callbackId() {
        return id;
    }

    @Override
    public final CallbackMessage linkCallback(int callbackId) {
        this.id = callbackId;
        return this;
    }

    @Override
    public Object returnData() {
        if (returnData.getClass() == EMPTY.getClass()) {
            return null;
        }
        return returnData;
    }

    @Override
    public final void readyOnCall(ICallback callback, AtomicInteger timeout) {
        this.callbackObj = callback;
        this.timeout = timeout;
    }

    @Override
    public long senderActorId() {
        if (actorId <= 0L) {
            return 0L;
        }
        return actorId;
    }

    public CallbackMessage senderActor(long actorId) {
        this.actorId = actorId;
        return this;
    }

    void data(Object data) {
        if (this.returnData == null) {
            if (data == null) {
                this.returnData = EMPTY;
            } else {
                this.returnData = data;
            }
        }
    }

    boolean isOriginal() {
        return returnData != null;
    }

}
