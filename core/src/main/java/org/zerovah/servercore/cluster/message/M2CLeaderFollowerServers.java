package org.zerovah.servercore.cluster.message;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class M2CLeaderFollowerServers implements KryoSerializable {

    private TreeMap<Integer, InetSocketAddress> masterAddrs = new TreeMap<>();

    private int connectedMasterId;

    public M2CLeaderFollowerServers() {
    }

    public Map<Integer, InetSocketAddress> getMasterAddrs() {
        return masterAddrs;
    }

    public int getConnectedMasterId() {
        return connectedMasterId;
    }

    public void fillAddressInfo(int masterId, InetSocketAddress masterAddr) {
        masterAddrs.put(masterId, masterAddr);
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(masterAddrs.size());
        for (Entry<Integer, InetSocketAddress> entry : masterAddrs.entrySet()) {
            output.writeInt(entry.getKey());
            InetSocketAddress socketAddr = entry.getValue();
            output.writeString(socketAddr.getAddress().getHostAddress());
            output.writeInt(socketAddr.getPort());
        }
        output.writeInt(connectedMasterId);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        int addressNum = input.readInt();
        for (int i = 0; i < addressNum; i++) {
            int masterId = input.readInt();
            String masterIp = input.readString();
            int port = input.readInt();
            masterAddrs.put(masterId, new InetSocketAddress(masterIp, port));
        }
        connectedMasterId = input.readInt();
    }
    
    
    public static M2CLeaderFollowerServers create(int masterId) {
        M2CLeaderFollowerServers message = new M2CLeaderFollowerServers();
        message.connectedMasterId = masterId;
        return message;
    }

}
