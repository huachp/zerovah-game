package org.zerovah.servercore.net.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class KryoDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = LogManager.getLogger(KryoDecoder.class);

    private static final FastThreadLocal<Kryo> KRYO_CACHE = new FastThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() throws Exception {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            return kryo;
        }

        @Override
        protected void onRemoval(Kryo value) throws Exception {
            LOGGER.info("解码器触发kryo对象删除");
        }
    };

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Kryo kryo = KRYO_CACHE.get();
        Input input = new Input(new ByteBufInputStream(in));
        Object message = kryo.readClassAndObject(input);
        input.close();
        out.add(message);
    }

}
