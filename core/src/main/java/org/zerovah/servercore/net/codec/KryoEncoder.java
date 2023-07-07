package org.zerovah.servercore.net.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.concurrent.FastThreadLocal;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KryoEncoder extends MessageToByteEncoder<Object> {

    private static final Logger LOGGER = LogManager.getLogger(KryoEncoder.class);

    private static final FastThreadLocal<Kryo> KRYO_CACHE = new FastThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() throws Exception {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            return kryo;
        }

        @Override
        protected void onRemoval(Kryo value) throws Exception {
            LOGGER.info("编码器触发kryo对象删除");
        }
    };

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        Kryo kryo = KRYO_CACHE.get();
        Output output = new Output(new ByteBufOutputStream(out));
        kryo.writeClassAndObject(output, msg);
        output.flush();
        output.close();
    }

}
