package org.zerovah.servercore.cluster.message;

/**
 * 服务端逻辑绑定Actor消息
 *
 * @author huachp
 */
public class BindingActorMsg {

    private Long clusterActorId;

    public BindingActorMsg() {
    }

    public Long getClusterActorId() {
        return clusterActorId;
    }

    public static BindingActorMsg create(Long clusterActorId) {
        BindingActorMsg bindingMsg = new BindingActorMsg();
        bindingMsg.clusterActorId = clusterActorId;
        return bindingMsg;
    }

}
