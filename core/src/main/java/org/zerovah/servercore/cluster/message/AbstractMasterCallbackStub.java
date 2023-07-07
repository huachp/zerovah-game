package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.ICallback;

public class AbstractMasterCallbackStub extends AbstractMasterMsg {

    private int callbackId;
    private transient ICallback callback;

    public int getCallbackId() {
        return callbackId;
    }

    public ICallback getCallback() {
        return callback;
    }

    public void retainStub(ICallback callback) {
        this.callback = callback;
    }

    public void injectId(int callbackId) {
        this.callbackId = callbackId;
    }
}
