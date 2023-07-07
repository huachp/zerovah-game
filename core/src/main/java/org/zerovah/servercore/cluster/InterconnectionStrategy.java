package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.base.NodeMainType;

/**
 * 节点互连策略
 *
 * @author huachp
 */
public interface InterconnectionStrategy {

    NodeMainType nodeType();

    boolean isConnectGate();

    boolean isConnectBusiness();

    boolean isConnectDbProxy();

}
