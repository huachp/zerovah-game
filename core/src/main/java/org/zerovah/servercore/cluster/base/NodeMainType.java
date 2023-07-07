package org.zerovah.servercore.cluster.base;

import org.zerovah.servercore.cluster.InterconnectionStrategy;

/**
 * 节点类型（大类）
 *
 * @author huachp
 */
public enum NodeMainType {

    GATE {
        @Override
        public boolean isConnectNode(InterconnectionStrategy strategy) {
            return strategy.isConnectGate();
        }
    },
    BUSINESS {
        @Override
        public boolean isConnectNode(InterconnectionStrategy strategy) {
            return strategy.isConnectBusiness();
        }
    },
    DBPROXY {
        @Override
        public boolean isConnectNode(InterconnectionStrategy strategy) {
            return strategy.isConnectDbProxy();
        }
    },
    RELAY {
        @Override
        public boolean isConnectNode(InterconnectionStrategy strategy) {
            return true;
        }
    }
    ;

    abstract boolean isConnectNode(InterconnectionStrategy strategy);

    public static boolean isInterConnect(
            InterconnectionStrategy selfStrategy, InterconnectionStrategy targetStrategy) {
        return selfStrategy.nodeType().isConnectNode(targetStrategy)
                && targetStrategy.nodeType().isConnectNode(selfStrategy);
    }

}
