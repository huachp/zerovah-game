package org.zerovah.servercore.cluster.actor;

import akka.actor.Actor;
import akka.actor.ActorRef;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.ThreadLocalRandom;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * 集群客户端通信父Actor
 *
 * @author huachp
 */
public class ParentActorIns {

    /** 随机函数 */
    private static final ThreadLocalRandom R = ThreadLocalRandom.current();
    /** 正在销毁 */
    private static final byte DESTROY_IN_PROGRESS = 1;
    /** 已销毁 */
    private static final byte DESTROYED_COMPLETELY = 2;


    /** 通信父Actor实例 */
    private Correspondence parentIns;
    /** 通信子Actor引用 */
    private LinkedList<ActorRef> childrenRefs = new LinkedList<>();
    /** 一致性hash通信Actor引用 */
    private ArrayList<ActorRef> consistentHashFixedRefs = new ArrayList<>();

    private volatile byte stageOfDestruction;

    private final int consistentHashActorCount;

    public ParentActorIns(Correspondence parentIns) {
        int fixedCount = NettyRuntime.availableProcessors();
        this.parentIns = parentIns;
        if (isPowerOfTwo(fixedCount)) {
            consistentHashActorCount = fixedCount;
        } else {
            consistentHashActorCount = toPowerOfTwo(fixedCount);
        }
    }

    public void initChildren(ActorRef first, ActorRef second) {
        childrenRefs.add(first);
        childrenRefs.add(second);
    }

    public Actor getParentIns() {
        return (Actor) parentIns;
    }

    public <T> T getActualActor(Class<T> cls) {
        return cls.cast(parentIns);
    }

    public Correspondence getCorrespondence() {
        return parentIns;
    }

    LinkedList<ActorRef> getChildrenRefs() {
        return childrenRefs;
    }

    public ActorRef chooseChildActor() {
        if ((R.nextInt(2) & 1) == 0)
            return childrenRefs.getFirst();
        else
            return childrenRefs.getLast();
    }

    public ActorRef chooseChildActor(int hashCode) {
        if (hashCode % 9 < 5)
            return childrenRefs.getFirst();
        else
            return childrenRefs.getLast();
    }

    public void destroyInProgress() {
        stageOfDestruction = DESTROY_IN_PROGRESS;
    }

    public void destroyedCompletely() {
        childrenRefs.clear();
        stageOfDestruction = DESTROYED_COMPLETELY;
    }

    public boolean isDestroyInProgress() {
        return stageOfDestruction == DESTROY_IN_PROGRESS;
    }

    public boolean isDestroyedCompletely() {
        return stageOfDestruction == DESTROYED_COMPLETELY;
    }

    public boolean isWorking() {
        return stageOfDestruction == 0;
    }

    public ActorRef getConsistentHashFixedActor(int hashKey) {
        int index = hashKey & (consistentHashActorCount - 1);
        ActorRef selectedActorRef = consistentHashFixedRefs.get(index);
        if (selectedActorRef == ActorRef.noSender()) {
            synchronized (this) {
                selectedActorRef = consistentHashFixedRefs.get(index);
                if (selectedActorRef != ActorRef.noSender()) {
                    return selectedActorRef;
                }
                selectedActorRef = parentIns.childActor();
                consistentHashFixedRefs.set(index, selectedActorRef);
            }
        }
        return selectedActorRef;
    }

//    public boolean addConsistentHashFixedActor(ActorRef childActor) {
//        if (consistentHashFixedRefs.size() >= consistentHashActorCount) {
//            return false;
//        }
//        synchronized (this) {
//            if (consistentHashFixedRefs.size() < consistentHashActorCount) {
//                consistentHashFixedRefs.add(childActor);
//                return true;
//            }
//            return false;
//        }
//    }

    private boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    private static int toPowerOfTwo(int val) {
        return Integer.highestOneBit(val) << 1;
    }


    public static ParentActorIns create(Correspondence parentIns) {
        ParentActorIns parentActor = new ParentActorIns(parentIns);
        for (int i = 0; i < parentActor.consistentHashActorCount; i++) {
            parentActor.consistentHashFixedRefs.add(ActorRef.noSender());
        }
        return parentActor;
    }

}
