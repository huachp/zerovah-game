package org.zerovah.servercore.cluster.base;

import org.zerovah.servercore.cluster.ICallback;
import org.zerovah.servercore.cluster.INodeFuture;

import java.util.Arrays;

/**
 * 抽象单节点逻辑成功后异步执行回调对象
 *
 * @author huachp
 */
public abstract class AbstractOneNodeFuture implements INodeFuture {

    protected ICallback[] listeners;
    protected int size; // 监听数量

    protected AbstractOneNodeFuture() {
        listeners = new ICallback[1];
    }

    @Override
    public INodeFuture addConnectedListener(int nodeType, ICallback callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public INodeFuture addListener(ICallback callback) {
        synchronized (this) {
            if (executeImmediately(callback)) {
                return this;
            }
            ICallback[] listeners = this.listeners;
            final int size = this.size;
            if (size == listeners.length) {
                this.listeners = listeners = Arrays.copyOf(listeners, size << 1);
            }
            listeners[size] = callback;
            this.size = size + 1;
            return this;
        }
    }

    public abstract boolean executeImmediately(ICallback callback);
}
