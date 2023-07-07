package org.zerovah.servercore.cluster.base;

import java.util.concurrent.atomic.AtomicInteger;

import static org.zerovah.servercore.cluster.master.MasterContext.INITIAL_ID_BITS;
import static org.zerovah.servercore.cluster.master.MasterContext.INITIAL_ID_SHIFT;

/**
 * Actor ID 生成器
 *
 * @author huachp
 */
public class ActorIdGenerator {

    public static final int NODEID_BITS = INITIAL_ID_BITS + INITIAL_ID_SHIFT;
    public static final int MAX_NODEID = (1 << NODEID_BITS) - 1;

    private final AtomicInteger increment = new AtomicInteger();

    public ActorIdGenerator() {
    }

    /**
     * 生成actorId, 注意: 传入的nodeId是自身节点的ID, 不能传入其他节点的ID生成actorId, 否则后果自负
     *
     * @param nodeId 本地节点ID, 必须是本地节点ID
     * @return {@link Long}
     */
    // FIXME 参数设计不好, 此接口未来可能会修改
    public long nextId(int nodeId) {
        long value = increment.incrementAndGet() & Integer.MAX_VALUE;
        return value << NODEID_BITS | nodeId;
    }


    public static int parseOutNodeId(long actorId) {
        return (int) (actorId & MAX_NODEID);
    }


//    public static void main(String[] args) {
//        int nodeId = 1 << 6 | 63;
//        System.out.println(nodeId);
//        ActorIdGenerator generator = new ActorIdGenerator();
//        for (int i = 0; i < 1000; i++) {
//            long actorId = generator.nextId(nodeId);
//            long hash = actorId ^ (actorId >>> 16);
//            System.out.println("ActorId[" + actorId + "]产生hash[" + hash + "]");
//            long index = (hash % 8) ^ 1;
//            System.out.println("随机位置：" + index);
//        }
//    }

}
