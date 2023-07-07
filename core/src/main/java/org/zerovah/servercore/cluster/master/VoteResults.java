package org.zerovah.servercore.cluster.master;

import java.util.HashMap;
import java.util.HashSet;

/**
 * 故障投票结果
 *
 * @author huachp
 */
public class VoteResults {

    private int nodeId;
    private int expectedCountOfVotes;
    private HashMap<Integer, Integer> initiatorCountMap = new HashMap<>();
    private HashSet<Integer> voteNodes = new HashSet<>();
    private long voteTime;
    private boolean notifiedNodes;

    public VoteResults(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public int getExpectedCountOfVotes() {
        return expectedCountOfVotes;
    }

    public HashSet<Integer> getVoteNodes() {
        return voteNodes;
    }

    public void increaseVotes(int node) {
        voteNodes.add(node);
    }

    public long getVoteTime() {
        return voteTime;
    }

    public boolean isVotePassed() {
        return voteNodes.size() >= expectedCountOfVotes;
    }

    public void nodeFaultAndNoitiedAllNodes() {
        this.notifiedNodes = true;
    }

    public boolean isNotifiedNodes() {
        return notifiedNodes;
    }

    public void increaseInitiatorCount(int node) {
        initiatorCountMap.merge(node, 1, Integer::sum);
    }

    public boolean isBroadcastVote() {
        if (expectedCountOfVotes > 0
                && initiatorCountMap.size() >= expectedCountOfVotes) {
            expectedCountOfVotes = initiatorCountMap.size();
            return true;
        }
        if (initiatorCountMap.size() <= 1) {
            return false;
        }
        for (Integer count : initiatorCountMap.values()) {
            if (count <= 1) {
                return false;
            }
        }
        expectedCountOfVotes = initiatorCountMap.size();
        return true;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        return b.append('{')
                .append("id=").append(nodeId)
                .append(", expectedCount=").append(expectedCountOfVotes)
                .append(", voteCount=").append(voteNodes.size())
                .append('}')
                .toString();
    }


    public static VoteResults create(int node, int expectedCount) {
        VoteResults results = new VoteResults(node);
        results.expectedCountOfVotes = expectedCount;
        results.voteTime = System.currentTimeMillis();
        return results;
    }

}
