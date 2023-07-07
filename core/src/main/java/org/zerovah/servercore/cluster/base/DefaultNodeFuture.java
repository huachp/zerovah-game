package org.zerovah.servercore.cluster.base;

import org.zerovah.servercore.cluster.ICallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 默认节点逻辑成功后回调事件
 *
 * @author huachp
 */
public class DefaultNodeFuture extends AbstractOneNodeFuture {

    private static final Logger LOGGER = LogManager.getLogger(DefaultNodeFuture.class);
    
    private static final Object EMPTY = new Object();

    private volatile Object result = EMPTY;

    public boolean setAndExecute(Object data) {
        synchronized (this) {
            this.result = data;
            ICallback[] listeners = this.listeners;
            for (int i = 0; i < size; i++) {
                try {
                    ICallback callback = listeners[i];
                    callback.onCall(data);
                    listeners[i] = null;
                } catch (Exception e) {
                    LOGGER.error("回调逻辑出现异常", e);
                }
            }
            size = 0; // 执行完毕, 清空回调, 不允许重复回调
        }
        return true;
    }


    @Override
    public boolean executeImmediately(ICallback callback) {
       if (result != EMPTY) {
           callback.onCall(result);
           return true;
       }
       return false;
    }

    public boolean isDone() {
        return result != EMPTY; // 无论是出错还是正常返回, 执行都算完成
    }

    public Object get() {
        return result != EMPTY ? result : this;
    }

}
