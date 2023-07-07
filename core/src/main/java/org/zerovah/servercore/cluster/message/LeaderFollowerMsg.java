package org.zerovah.servercore.cluster.message;

public class LeaderFollowerMsg {

    private MasterMessage masterMsg;
    private Object attachment;

    public MasterMessage getMasterMsg() {
        return masterMsg;
    }

    public Object getAttachment() {
        return attachment;
    }

    public <T> T actualTypeOfObj() {
        return (T) attachment;
    }

    public void attach(Object objInfo) {
        this.attachment = objInfo;
    }
    
    
    public static LeaderFollowerMsg create(MasterMessage masterMsg) {
        LeaderFollowerMsg message = new LeaderFollowerMsg();
        message.masterMsg = masterMsg;
        return message;
    }
}
