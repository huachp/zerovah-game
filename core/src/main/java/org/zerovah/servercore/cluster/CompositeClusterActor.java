package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.message.RemoteMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// FIXME: 临时用, 未来会删除
// @Deprecated
public class CompositeClusterActor {
    
    private static final Logger LOGGER = LogManager.getLogger(CompositeClusterActor.class);

    @Deprecated
    static final ConcurrentMap<Integer, CompositeClusterActor> compositeActors = new ConcurrentHashMap<>();


    private MixedNodeActor mixedNodeActor;

    public void remoteTell(RemoteMessage message) {
        if (mixedNodeActor == MixedNodeActor.DEFAULT) {
            LOGGER.warn("CompositeClusterActor广播消息{}, 找不到可用节点", message);
        }
        mixedNodeActor.tellAll(message);
    }


    @Deprecated
    static CompositeClusterActor get(Integer nodeType, MixedNodeActor mixedActor) {
        CompositeClusterActor compositeActor = compositeActors.get(nodeType);
        if (compositeActor == null) {
            compositeActor = CompositeClusterActor.create(mixedActor);
            CompositeClusterActor old = compositeActors.putIfAbsent(nodeType, compositeActor);
            if (old != null) {
                compositeActor = old;
            }
        }
        return compositeActor;
    }

    @Deprecated
    public static CompositeClusterActor create(MixedNodeActor mixedActor) {
        CompositeClusterActor compositeActor = new CompositeClusterActor();
        compositeActor.mixedNodeActor = mixedActor;
        return compositeActor;
    }
}
