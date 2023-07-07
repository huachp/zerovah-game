package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.ICallback;

import java.util.concurrent.atomic.AtomicInteger;

public interface CallbackMessage {

    int callbackId();

    CallbackMessage linkCallback(int callbackId);

    Object returnData();

    void readyOnCall(ICallback callbackObj, AtomicInteger timeout);

    boolean callback();

    long senderActorId();

}
