package org.zerovah.servercore.cluster;

import org.zerovah.servercore.cluster.actor.Correspondence;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机选择节点路由策略, 已排除本地节点
 *
 * @author huachp
 */
public class RandomRouteStrategy implements RouteStrategy<Correspondence> {

    @Override
    public Correspondence route(Collection<Correspondence> actors, int key) {
        if (actors.isEmpty()) {
            return null;
        }
        if (actors.size() == 1) {
            Correspondence actor = actors.iterator().next();
            return !actor.isFaultyNode() && actor.isNodeUsable() ? actor : null;
        }
        int cursor = ThreadLocalRandom.current().nextInt(0, actors.size());
        Iterator<Correspondence> it = actors.iterator();
        Correspondence selected = null;
        for (int i = 0; it.hasNext(); i++) {
            Correspondence actor = it.next();
            if (i == cursor) {
                selected = actor; break;
            }
        }
        if (selected == null) {
            return null;
        }
        if (selected.isFaultyNode() || !selected.isNodeUsable()) {
            return chooseNotFaultyActor(actors);
        }
        return selected;
    }

    Correspondence chooseNotFaultyActor(Collection<Correspondence> actors) {
        Iterator<Correspondence> it = actors.iterator();
        for (; it.hasNext(); ) {
            Correspondence actor = it.next();
            if (!actor.isFaultyNode() && actor.isNodeUsable()) { // 不故障且可用
                return actor;
            }
        }
        return null;
    }

}
