package org.zerovah.servercore.cluster.master;

/**
 * Raft状态, 有兴趣的同学可以去了解下Raft算法
 *
 * @author huachp
 */
public enum RaftState {

    LEADER,

    FOLLOWER,

}
