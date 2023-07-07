package org.zerovah.servercore.cluster.cause;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 节点异常
 *
 * @author huachp
 */
public class NodeException extends Throwable implements KryoSerializable {

    private static final String REMOTE_DETAIL = "remote call logic occur exception, detail: ";
    private static final String TIMEOUT_DETAIL = "remote call timeout exception, detail: ";

    private String detail;

    public NodeException() {
    }

    public NodeException(String detail) {
        this.detail = detail;
    }

    public String getDetail() {
        return detail;
    }

    @Override
    public Throwable fillInStackTrace() {
        // do nothing
        return this;
    }


    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(detail);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.detail = input.readString();
    }


    public static NodeException remoteFailure(String exceptionMsg) {
        return new NodeException(REMOTE_DETAIL + exceptionMsg);
    }

    public static NodeException remoteTimeout(String exceptionMsg) {
        return new NodeException(TIMEOUT_DETAIL + exceptionMsg);
    }

}
