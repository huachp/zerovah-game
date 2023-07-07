package org.zerovah.servercore.cluster.message;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.zerovah.servercore.cluster.NodeLocatedIpGroup;
import org.zerovah.servercore.cluster.master.NodeData;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * 节点连接数据信息（Master）
 *
 * @author huachp
 */
public class M2CNodeListMsg implements KryoSerializable {

    private LinkedList<NodeData> dataList;

    private ArrayList<NodeLocatedIpGroup> ipGroups; // 节点IP组配置

    public M2CNodeListMsg() {
        this.dataList = new LinkedList<>();
    }

    public void addNode(NodeData nodeData) {
        dataList.addLast(nodeData);
    }

    public LinkedList<NodeData> getDataList() {
        return dataList;
    }

    public ArrayList<NodeLocatedIpGroup> getIpGroups() {
        return ipGroups;
    }


    public static M2CNodeListMsg create(ArrayList<NodeLocatedIpGroup> ipGroups) {
        M2CNodeListMsg nodeListMsg = new M2CNodeListMsg();
        nodeListMsg.ipGroups = ipGroups;
        return nodeListMsg;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(dataList.size());
        dataList.forEach(data -> {
            kryo.writeClassAndObject(output, data);
        });
        output.writeInt(ipGroups.size());
        ipGroups.forEach(data -> {
            kryo.writeClassAndObject(output, data);
        });
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int nodeDataSize = input.readInt();
        for (int i = 0; i < nodeDataSize; i++) {
            NodeData nodeData = (NodeData) kryo.readClassAndObject(input);
            dataList.add(nodeData);
        }
        int nodeIpSize = input.readInt();
        ipGroups = new ArrayList<>(nodeIpSize);
        for (int i = 0; i < nodeIpSize; i++) {
            NodeLocatedIpGroup ipGroup = (NodeLocatedIpGroup) kryo.readClassAndObject(input);
            ipGroups.add(ipGroup);
        }
    }

}
