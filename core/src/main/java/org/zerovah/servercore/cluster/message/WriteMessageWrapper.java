package org.zerovah.servercore.cluster.message;

public class WriteMessageWrapper {

    private int masterId; // -1是广播
    private Object message;

    public WriteMessageWrapper() {
    }

    public int getMasterId() {
        return masterId;
    }

    public Object getMessage() {
        return message;
    }


    public static WriteMessageWrapper create(int masterId, Object message) {
        WriteMessageWrapper messageWrapper = new WriteMessageWrapper();
        messageWrapper.masterId = masterId;
        messageWrapper.message = message;
        return messageWrapper;
    }

}
