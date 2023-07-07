package org.zerovah.servercore.cluster.message;

import java.util.Comparator;

public class CallbackTimeoutCDMsg {

    public static Comparator<CallbackTimeoutCDMsg> CD_COMPARATOR = comparator();

    private int callbackId;
    private int timeoutSeconds;
    private int endTime;
    private Class<? extends RemoteMessage> msgClass;

    public CallbackTimeoutCDMsg() {
    }

    public int getCallbackId() {
        return callbackId;
    }

    public int getEndTime() {
        return endTime;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public boolean isTimeout() {
        long now = System.currentTimeMillis();
        return (int) (now / 1000) >= endTime;
    }

    public Class<? extends RemoteMessage> getMsgClass() {
        return msgClass;
    }

    @Override
    public String toString() {
        return '{' +
                "callbackId=" + callbackId +
                ", timeoutSeconds=" + timeoutSeconds +
                ", msgClass=" + msgClass.getSimpleName() +
                '}';
    }


    public static CallbackTimeoutCDMsg create(int callbackId, RemoteMessage msg) {
        CallbackTimeoutCDMsg message = new CallbackTimeoutCDMsg();
        message.callbackId = callbackId;
        int startTime = (int) (System.currentTimeMillis() / 1000);
        message.timeoutSeconds = msg.getTimeoutTime();
        message.endTime = startTime + msg.getTimeoutTime();
        message.msgClass = msg.getClass();
        return message;
    }

    private static Comparator<CallbackTimeoutCDMsg> comparator() {
        return (cdObj1, cdObj2) -> {
            int flag = cdObj1.endTime - cdObj2.endTime;
            return flag != 0 ? flag : cdObj1.callbackId - cdObj2.callbackId;
        };
    }

}
