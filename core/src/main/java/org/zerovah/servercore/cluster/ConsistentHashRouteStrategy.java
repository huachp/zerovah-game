package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.actor.Correspondence;
import org.zerovah.servercore.cluster.actor.ParentActorIns;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * 一致性hash节点选择路由策略, 已排除本地节点<p>
 * 固定hash散列表长度为65536<p>
 * 注意: 一致性hash节点路由策略不能应用在选择和本地节点同类型的节点Actor
 *
 * @author huachp
 */
public class ConsistentHashRouteStrategy implements RouteStrategy<Correspondence> {
    
    private static final Logger LOGGER = LogManager.getLogger(ConsistentHashRouteStrategy.class);


    private ClusterContext clusterCtx;

    public ConsistentHashRouteStrategy(ClusterContext clusterCtx) {
        this.clusterCtx = clusterCtx;
    }

    @Override
    public Correspondence route(Collection<Correspondence> actors, int key) {
        if (actors.isEmpty()) {
            return null;
        }
        int hashTableLength = 65535; // 固定hash table长度65535
        LinkedList<Integer> hashTableIndexes = new LinkedList<>();
        Map<Integer, Correspondence> actorMap = new HashMap<>(actors.size());

        for (Correspondence parentActor : actors) {
            int h = hash(parentActor.connectedNodeId()); // 节点ID在整个集群中唯一
            int tableIndex = (hashTableLength - 1) & h;

            if (hashTableIndexes.isEmpty()) {
                hashTableIndexes.add(tableIndex);
                actorMap.put(tableIndex, parentActor);
            } else {
                int listIdx = Collections.binarySearch(hashTableIndexes, tableIndex);
                if (listIdx >= 0) {
                    LOGGER.warn("节点id[{}]路由逻辑出现hash冲突", parentActor.connectedNodeId());
                    continue;
                }
                listIdx = Math.abs(listIdx) - 1;
                hashTableIndexes.add(listIdx, tableIndex);
                actorMap.put(tableIndex, parentActor);
            }
        }
        int h = hash(key);
        int tableIndex = (hashTableLength - 1) & h;
        int listIdx = Collections.binarySearch(hashTableIndexes, tableIndex);
        if (listIdx < 0) {
            listIdx = Math.abs(listIdx) - 1;
        }

        Correspondence selectedActor;
        if (listIdx >= hashTableIndexes.size()) {
            selectedActor = selectAvailableActor(hashTableIndexes, actorMap);
        } else {
            selectedActor = selectAvailableActor(hashTableIndexes, actorMap, listIdx);
        }
        actorMap.clear();
        return selectedActor;
    }

    private Correspondence selectAvailableActor(LinkedList<Integer> hashTableIndexes, Map<Integer, Correspondence> actorMap) {
        int size = hashTableIndexes.size();
        for (int i = 0; i < size; i++) {
            Correspondence selectedActor = actorMap.get(hashTableIndexes.poll());
            ParentActorIns actorIns = clusterCtx.getParentActor(selectedActor.connectedNodeId());
            if (actorIns != null && !actorIns.isWorking()) {
                continue;
            }
            return selectedActor;
        }
        return null;
    }

    private Correspondence selectAvailableActor(LinkedList<Integer> hashTableIndexes, Map<Integer, Correspondence> actorMap, int listIdx) {
        for (int i = listIdx; i < hashTableIndexes.size(); i++) {
            Integer selectedTabIdx = hashTableIndexes.get(i);
            Correspondence selectedActor = actorMap.get(selectedTabIdx);

            ParentActorIns actorIns = clusterCtx.getParentActor(selectedActor.connectedNodeId());
            if (actorIns != null && !actorIns.isWorking()) {
                continue;
            }
            return selectedActor;
        }
        for (int i = 0; i < listIdx; i++) {
            Integer selectedTabIdx = hashTableIndexes.get(i);
            Correspondence selectedActor = actorMap.get(selectedTabIdx);

            ParentActorIns actorIns = clusterCtx.getParentActor(selectedActor.connectedNodeId());
            if (actorIns != null && !actorIns.isWorking()) {
                continue;
            }
            return selectedActor;
        }
        return null;
    }


    private int hash(Object objKey) {
        int h;
        return (objKey == null) ? 0 : (h = objKey.hashCode() * 101) ^ (h >>> 16);
    }


}
