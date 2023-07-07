package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.master.RaftState;

/**
 * 节点初始化返回消息（Master）
 *
 * @author huachp
 */
public class M2CInitNodeMsg {

    /**
     * 只有{@link RaftState#LEADER}状态的Master服务器才会返回nodeId和port信息
     */

    /* ---------------- Fields -------------- */

    /** 节点ID */
    private int nodeId;
    /** 节点类型 */
    private int nodeType;
    /** 节点端口 */
    private int port;
    /** Master服务标识 */
    private int masterId;
    /** Master服务主从状态 */
    private RaftState state;
    /** 初始化冲突 */
    private boolean conflict;

    public M2CInitNodeMsg() {

    }

    public M2CInitNodeMsg(int masterId, RaftState state) {
        this.masterId = masterId;
        this.state = state;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getNodeType() {
        return nodeType;
    }

    public int getPort() {
        return port;
    }

    public int getMasterId() {
        return masterId;
    }

    public RaftState getState() {
        return state;
    }

    public boolean isConflict() {
        return conflict;
    }

    public M2CInitNodeMsg attach(int nodeId, int nodeType, int port) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.port = port;
        return this;
    }

    public M2CInitNodeMsg dataConflit(int nodeId) {
        this.conflict = true;
        this.nodeId = nodeId;
        return this;
    }


    public static M2CInitNodeMsg create(int masterId, RaftState state) {
        return new M2CInitNodeMsg(masterId, state);
    }
}
