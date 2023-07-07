package org.zerovah.servercore.cluster;

import java.util.Collection;

/**
 * 集群路由策略
 *
 * @author huachp
 */
public interface RouteStrategy<A> {

    A route(Collection<A> actors, int key);
}
