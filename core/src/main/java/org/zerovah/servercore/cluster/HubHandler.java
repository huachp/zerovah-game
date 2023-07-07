package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.actor.PeersActorContext;
import org.zerovah.servercore.cluster.message.CallbackMessage;
import org.zerovah.servercore.cluster.message.RemoteMessage;

/**
 * 集群逻辑中心或转发处理接口
 *
 * @author huachp
 */
public interface HubHandler {

    void forward(RemoteMessage remoteMsg, PeersActorContext ctx);

    void forward(CallbackMessage callbackMsg);
}
