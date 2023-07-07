package org.zerovah.servercore.cluster.message;

import org.zerovah.servercore.cluster.cause.NodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 默认回调消息
 *
 * @author huachp
 */
public class SimpleCallbackMsg extends AbstractCallbackMsg {
    
    private static final Logger LOGGER = LogManager.getLogger(SimpleCallbackMsg.class);

    SimpleCallbackMsg() {}

    public boolean callback() {
        if (callbackObj != null) {
            synchronized (callbackObj) {
                return doCallback();
            }
        }
        return false;
    }

    private boolean doCallback() {
        if (timeout.get() > 0) { // 大于0是超时状态
            return true;
        }
        if (isOriginal()) {
            Object responseData = returnData();
            if (responseData instanceof NodeException) {
                NodeException nodeEx = (NodeException) responseData;
                LOGGER.info("远程消息回调异常:{}", nodeEx.getDetail());
            }
            callbackObj.onCall(responseData);
        } else {
            callbackObj.onCall(this);
        }
        timeout.getAndSet(-1);
        return true;
    }


    public static SimpleCallbackMsg create(Object returnData) {
        SimpleCallbackMsg callbackMsg = new SimpleCallbackMsg();
        callbackMsg.data(returnData);
        return callbackMsg;
    }

}
