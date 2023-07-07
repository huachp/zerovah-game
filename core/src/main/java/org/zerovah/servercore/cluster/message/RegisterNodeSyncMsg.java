package org.zerovah.servercore.cluster.message;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.zerovah.servercore.cluster.master.RegisteredNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Master服务注册节点同步消息
 *
 * @author huachp
 */
public class RegisterNodeSyncMsg implements KryoSerializable {

    /** 当前节点自增量 */
    private int currentNodeIncrement;
    /** 同步已注册的节点信息 */
    private Map<Integer, RegisteredNode> syncNodes;

    public RegisterNodeSyncMsg() {
    }

    public int getCurrentNodeIncrement() {
        return currentNodeIncrement;
    }

    public Map<Integer, RegisteredNode> getSyncNodes() {
        return syncNodes;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(syncNodes.size());
        syncNodes.forEach((k, v) -> {
            kryo.writeClassAndObject(output, v);
        });
    }

    @Override
    public void read(Kryo kryo, Input input) {
        HashMap<Integer, RegisteredNode> nodesMap = new HashMap<>();
        int size = input.readInt();
        for (int i = 0; i < size; i++) {
            RegisteredNode node = (RegisteredNode) kryo.readClassAndObject(input);
            nodesMap.put(node.nodeId(), node);
        }
        syncNodes = nodesMap;
    }


    public static RegisterNodeSyncMsg create(int nodeIncrement, Map<Integer, RegisteredNode> nodes) {
        RegisterNodeSyncMsg msg = new RegisterNodeSyncMsg();
        msg.currentNodeIncrement = nodeIncrement;
        msg.syncNodes = nodes;
        return msg;
    }

}
