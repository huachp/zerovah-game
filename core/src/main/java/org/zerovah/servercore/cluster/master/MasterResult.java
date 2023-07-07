package org.zerovah.servercore.cluster.master;

import java.util.Objects;

public class MasterResult {

    private long timestamp;
    private int timeout;

    private Object r;

    public long getTimestamp() {
        return timestamp;
    }

    public int getTimeout() {
        return timeout;
    }

    public <T> T get() {
        return (T) r;
    }

    public void set(Object r) {
        Objects.requireNonNull(r);
        this.r = r;
    }

    public boolean isSuccess() {
        return r != null;
    }


    public static MasterResult create(int timeout) {
        MasterResult result = new MasterResult();
        result.timestamp = System.currentTimeMillis();
        result.timeout = timeout;
        return result;
    }
}
