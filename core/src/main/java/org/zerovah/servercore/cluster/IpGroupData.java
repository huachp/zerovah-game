package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.actor.Correspondence;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 物理服务器IP分组(根据地区分组)信息
 *
 * @author huachp
 */
public class IpGroupData {

    private static final Logger LOGGER = LogManager.getLogger(IpGroupData.class);

    private static final ThreadLocal<LinkedList> LOCAL_OBJECTS = ThreadLocal.withInitial(LinkedList::new);
    
    /** 节点IP分组固定配置 {IP=IP地址组信息列表} */
    private Map<String, LinkedList<NodeLocatedIpGroup>> ipGroups;
    /** 分组ID对IP列表映射 {groupId=P地址组信息列表} */
    private Map<Integer, LinkedList<NodeLocatedIpGroup>> groupIdAndIpsMap;

    public IpGroupData() {

    }

    public Map<String, LinkedList<NodeLocatedIpGroup>> getIpGroups() {
        return ipGroups;
    }

    public boolean containsIp(String ipAddress) {
        return ipGroups.containsKey(ipAddress.trim());
    }

    public boolean isEmpty() {
        return ipGroups == null || ipGroups.isEmpty();
    }

    private LinkedList<Correspondence> getEmptyListFromThread() {
        LinkedList actorList = LOCAL_OBJECTS.get();
        actorList.clear();
        return actorList;
    }

    private LinkedList<Correspondence> groupAvailableActors(
            List<NodeLocatedIpGroup> groupIpList, Collection<Correspondence> actors) {
        LinkedList<Correspondence> actorList = getEmptyListFromThread();
        for (Correspondence actor : actors) {
            if (containsGroupIp(groupIpList, actor.connectedIp())
                    && !actor.isFaultyNode() && actor.isNodeUsable()) {
                actorList.add(actor);
            }
        }
        return actorList;
    }

    private boolean containsGroupIp(List<NodeLocatedIpGroup> groupIpList, String ip) {
        for (NodeLocatedIpGroup ipGroup : groupIpList) {
            if (ipGroup.getFormatIp().equals(ip)) {
                return true;
            }
        }
        return false;
    }

    public Correspondence randomAvailableActor(String localIp, Collection<Correspondence> actors) {
        List<NodeLocatedIpGroup> groupIpList = ipGroups.get(localIp.trim());
        if (groupIpList == null) {
            return null;
        }
        LinkedList<Correspondence> actorList = groupAvailableActors(groupIpList, actors);
        if (actorList.isEmpty()) {
            return null;
        }
        if (actorList.size() == 1) {
            return actorList.poll();
        }
        int idx = ThreadLocalRandom.current().nextInt(0, actorList.size());
        Correspondence selectedActor = actorList.get(idx);
        actorList.clear(); // 一定要清理
        return selectedActor;
    }

    public int getGroupId(String ip) {
        LinkedList<NodeLocatedIpGroup> nodeIpList = ipGroups.get(ip);
        if (nodeIpList != null) {
            return nodeIpList.getFirst().getLocatedGroup();
        }
        return -1;
    }

    public String getOneOfGroupIp(int groupId) {
        LinkedList<NodeLocatedIpGroup> nodeIpList = groupIdAndIpsMap.get(groupId);
        if (nodeIpList == null)
            return "127.0.0.1";

        return nodeIpList.getFirst().getFormatIp();
    }

    public List<int[]> getGroupSlotRanges(String ip) {
        List<int[]> slotIds = new ArrayList<>();
        LinkedList<NodeLocatedIpGroup> nodeIpList = ipGroups.get(ip);
        if (nodeIpList == null) {
            return Collections.emptyList();
        }
        for (NodeLocatedIpGroup nodeGroup : nodeIpList) {
            int[] range = nodeGroup.getDbSlotRange();
            if (range != null) {
                slotIds.add(range);
            }
        }
        return slotIds;
    }

    public boolean compare(List<NodeLocatedIpGroup> ipGroupList) {
        return isEmpty() || ipGroupList.size() > this.ipGroups.size();
    }


    public static IpGroupData create(List<NodeLocatedIpGroup> rawData) {
        IpGroupData ipGroupData = new IpGroupData();
        ipGroupData.ipGroups = new HashMap<>();
        ipGroupData.groupIdAndIpsMap = new HashMap<>();
        Map<Integer, LinkedList<NodeLocatedIpGroup>> groupMap = ipGroupData.groupIdAndIpsMap;
        for (NodeLocatedIpGroup nodeGroup : rawData) {
            Integer group = nodeGroup.getLocatedGroup();
            LinkedList<NodeLocatedIpGroup> nodeIpList = groupMap.computeIfAbsent(group, k -> new LinkedList<>());
            nodeIpList.add(nodeGroup);
            ipGroupData.ipGroups.put(nodeGroup.getFormatIp(), nodeIpList);
        }
        LOGGER.info("更新IP分组数据:{}", groupMap.values());
        return ipGroupData;
    }

}
