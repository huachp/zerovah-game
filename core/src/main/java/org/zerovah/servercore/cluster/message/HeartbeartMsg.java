package org.zerovah.servercore.cluster.message;

/**
 * 心跳消息
 *
 * @author huachp
 */
public class HeartbeartMsg {

    private short heartbeatId;
    private byte state; // 请求响应状态
    private transient boolean timeout;

    public HeartbeartMsg() {
    }

    public short getHeartbeatId() {
        return heartbeatId;
    }

    public int getIdInt() {
        return heartbeatId;
    }

    public HeartbeartMsg response() {
        this.state = 1;
        return this;
    }

    public HeartbeartMsg injectId(int heartbeatId) {
        short loopId = (short) (heartbeatId & Short.MAX_VALUE);
        this.heartbeatId = loopId;
        return this;
    }

    public boolean isRequest() {
        return state == 0;
    }

    public boolean isResponse() {
        return state == 1;
    }

    public HeartbeartMsg timeout() {
        this.timeout = true;
        this.state = -1; // 超时要重置状态
        return this;
    }

    public boolean isTimeout() {
        return timeout;
    }


    public static HeartbeartMsg create() {
        return new HeartbeartMsg();
    }

}
