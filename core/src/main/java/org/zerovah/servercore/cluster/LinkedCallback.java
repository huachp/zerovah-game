package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.cause.NodeException;

import java.util.Objects;

public class LinkedCallback implements ICallback {
    
//    private static final Logger LOGGER = LogManager.getLogger(LinkedCallback.class);

    private volatile Object result;

    private ChainNode head;

    private ChainNode tail;

    LinkedCallback() {
    }

    @Override
    public void onCall(Object data) {
        Objects.requireNonNull(data, "call back data must be not null");
        synchronized (this) {
            if (Objects.isNull(result))
                result = data;
            for (ChainNode current = head; current != null; ) {
                if (data instanceof Throwable) {
                    if (current.mode == EXCEPTIONALLY) {
                        current.item.onCall(data);
                    }
                } else {
                    if (current.mode == SMOOTHLY) {
                        current.item.onCall(data);
                    }
                }
                ChainNode discard = current;
                current = current.next;
                discard.next = null;
            }
            head = null;
            tail = null;
        }
    }

    void linkLast(ICallback callback, int mode) {
        if (head == null) {
            head = new ChainNode(callback, mode);
            tail = head;
        } else {
            tail.next = new ChainNode(callback, mode);
            tail = tail.next;
        }
    }

    @Override
    public LinkedCallback exceptionally(TypedCallback<NodeException> c) {
        synchronized (this) {
            if (Objects.nonNull(result)) {
                if (result instanceof Throwable) {
                    c.onCall(result);
                }
            } else {
                linkLast(c, EXCEPTIONALLY);
            }
        }
        return this;
    }

    @Override
    public LinkedCallback smoothly(ICallback c) {
        synchronized (this) {
            if (Objects.nonNull(result)) {
                if (!(result instanceof Throwable)) {
                    c.onCall(result);
                }
            } else {
                linkLast(c, SMOOTHLY);
            }
        }
        return this;
    }


    private static class ChainNode {

        int mode;
        ICallback item;
        ChainNode next;

        ChainNode(ICallback item, int mode) {
            this.item = item;
            this.mode = mode;
        }
    }

}
