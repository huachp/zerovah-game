package org.zerovah.servercore.cluster;

public interface TypedCallback<T> extends ICallback {

    void act(T typeData);

    @Override
    default void onCall(Object data) {
        T actualTypeData = (T) data;
        act(actualTypeData);
    }
}
