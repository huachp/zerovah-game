package org.zerovah.servercore.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.zerovah.servercore.cluster.master.MasterActorsPool;
import org.zerovah.servercore.cluster.master.NodeData;
import org.zerovah.servercore.cluster.message.NodeFaultMsg;

import java.util.LinkedList;

/**
 * 集群上下文接口访问权限
 *
 * @author huachp
 */
public class ContextAccessRights {

    private static final String PACKAGE_PATH = "org.zerovah.servercore";

    private int stackCursor;

    ContextAccessRights() {

    }

    private ContextAccessRights previousStack() {
        stackCursor ++;
        return this;
    }

    private int cursor() {
        return stackCursor;
    }

    public String clientActorParentName(ClusterContext clusterCtx) {
        return clusterCtx._clientActorParentName();
    }

    public ActorRef getClusterActorByNodeId(ClusterContext clusterCtx, Integer nodeId) {
        return clusterCtx.getClusterActorByNodeId(nodeId);
    }

    public ActorRef getClusterActorByNodeId(ClusterContext clusterCtx, Integer nodeId, int hash) {
        return clusterCtx.getClusterActorByNodeId(nodeId, hash);
    }

    public ActorRef getNewChildActor(ClusterContext clusterCtx, Integer nodeType) {
        return clusterCtx.getNewChildActor(nodeType);
    }

    public void nodeFailure(ClusterContext clusterCtx, NodeFaultMsg faultMsg) {
        clusterCtx.nodeFailure(faultMsg);
    }

    public void contrastSurvivingNodesFromMaster(ClusterContext clusterCtx, LinkedList<NodeData> nodes) {
        clusterCtx.contrastSurvivingNodesFromMaster(nodes);
    }

    public MasterActorsPool getMasteActorPool(ClusterContext clusterCtx) {
        return clusterCtx.getMasteActorPool();
    }

    public ActorSystem clusterSystem(ClusterContext clusterCtx) {
        return clusterCtx.clusterSystem();
    }


    public static ContextAccessRights access() {
        ContextAccessRights ctxAccessR = new ContextAccessRights();
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int cursor = ctxAccessR.previousStack().previousStack().cursor();
        StackTraceElement ste =  stackTrace[cursor];
        String callerClsName = ste.getClassName();
        if (callerClsName.startsWith(PACKAGE_PATH)) {
            return ctxAccessR;
        } else {
            throw new RuntimeException(callerClsName + "对ContextAccessRights无权限访问");
        }
    }

}
