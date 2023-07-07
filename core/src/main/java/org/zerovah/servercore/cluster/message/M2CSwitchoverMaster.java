package org.zerovah.servercore.cluster.message;

public class M2CSwitchoverMaster {

    private int masterId;
    private boolean leaderOrNot;

    public int getMasterId() {
        return masterId;
    }

    public boolean isLeaderOrNot() {
        return leaderOrNot;
    }

    public M2CSwitchoverMaster ofLeaderOrNot(boolean yesOrNot) {
        this.leaderOrNot = yesOrNot;
        return this;
    }


    public static M2CSwitchoverMaster create(int masterId) {
        M2CSwitchoverMaster message = new M2CSwitchoverMaster();
        message.masterId = masterId;
        return message;
    }
}
