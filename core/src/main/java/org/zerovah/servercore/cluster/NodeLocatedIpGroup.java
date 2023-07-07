package org.zerovah.servercore.cluster;

public class NodeLocatedIpGroup {

    private int id = 0;
    private String ip = "";
    private int locatedGroup = 0;

    private int dbSlotLeftInterval;
    private int dbSlotRightInterval;

    public int getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getLocatedGroup() {
        return locatedGroup;
    }

    public String getFormatIp() {
        return ip.trim();
    }

    public int getDbSlotLeftInterval() {
        return dbSlotLeftInterval;
    }

    public int getDbSlotRightInterval() {
        return dbSlotRightInterval;
    }

    public int[] getDbSlotRange() {
        return new int[] { dbSlotLeftInterval, dbSlotRightInterval };
    }

    
    public static NodeLocatedIpGroup create(int id, String ip, int ipGroup, int slotLeft, int slotRight) {
        NodeLocatedIpGroup nodeGroup = new NodeLocatedIpGroup();
        nodeGroup.id = id;
        nodeGroup.ip = ip;
        nodeGroup.locatedGroup = ipGroup;
        nodeGroup.dbSlotLeftInterval = slotLeft;
        nodeGroup.dbSlotRightInterval = slotRight;
        return nodeGroup;
    }
}
