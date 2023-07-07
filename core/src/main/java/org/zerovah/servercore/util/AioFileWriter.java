package org.zerovah.servercore.util;

import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Aio异步写文件
 *
 * @author huachp
 */
public class AioFileWriter {
    
    private static final Logger LOGGER = LogManager.getLogger(AioFileWriter.class);
    
    // 异步写文件不能使用 try-with-resource 的代码方式
    public static void flushFile(String filePath, byte[] byteStream, boolean append) {
        AsynchronousFileChannel fileChannel = null;
        try {
            final Path path = Paths.get(filePath);
            if (!append) {
                Files.deleteIfExists(path);
            }
            if (!Files.exists(path)) {
                final Path parentDir = path.getParent();
                if (parentDir != null && !Files.exists(parentDir)) {
                    Files.createDirectories(parentDir);
                }
                Files.createFile(path);
            }

            fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
            ByteBuffer buffer = ByteBuffer.wrap(byteStream);
            long position = 0;
            if (append) {
                position = fileChannel.size();
            }
            buffer.position(byteStream.length);
            buffer.flip();

            final AsynchronousFileChannel needClosedChannel = fileChannel;
            fileChannel.write(buffer, position, buffer, new CompletionHandler<Integer, ByteBuffer>() {

                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    // 文件写成功后, 关闭流
                    closeChannel(needClosedChannel);
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    LOGGER.error("文件写入出错", exc);
                    // 失败, 也关闭流
                    closeChannel(needClosedChannel);
                }
            });

        } catch (Exception e) {
            LOGGER.error("写文件出错了, {}", filePath, e);
            closeChannel(fileChannel);
        }
    }

    private static void closeChannel(Channel fileChannel) {
        try {
            if (fileChannel != null) {
                fileChannel.close();
            }
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    public static ContinuesWriteDataHandler openFile(String filePath) {
        AsynchronousFileChannel fileChannel = null;
        Path path = Paths.get(filePath);
        try {
            if (!Files.exists(path)) {
                final Path parentDir = path.getParent();
                if (parentDir != null && !Files.exists(parentDir)) {
                    Files.createDirectories(parentDir);
                }
                Files.createFile(path);
            }
            fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.WRITE);
        } catch (Exception e) {
            LOGGER.error("打开文件出错, {}", filePath, e);
            closeChannel(fileChannel);
            throw new NullPointerException("AsynchronousFileChannel初始化失败");
        }
        return new ContinuesWriteDataHandler(fileChannel, path.toAbsolutePath().toString());
    }

//    public static void continuousWrite(AsynchronousFileChannel openedChannel, String content) {
//        try {
//            if (!openedChannel.isOpen()) {
//                LOGGER.debug("文件尚未打开");
//                return;
//            }
//            byte[] byteStream = content.getBytes(CharsetUtil.UTF_8);
//            ByteBuffer buffer = ByteBuffer.wrap(byteStream);
//            long position = openedChannel.size();
//            buffer.position(byteStream.length);
//            buffer.flip();
//
//            CompletionHandler<Integer, Channel> c = ContinuesWriteDataHandler.get(openedChannel).submitWrite();
//            openedChannel.write(buffer, position, openedChannel, c);
//        } catch (Exception e) {
//            LOGGER.error("持续写入文件出错", e);
//        }
//
//    }


    public static class ContinuesWriteDataHandler implements CompletionHandler<Integer, Channel> {

        private AtomicInteger writeCounter = new AtomicInteger();
        private AtomicInteger completeCounter = new AtomicInteger();
        private AtomicBoolean readyClose = new AtomicBoolean();

        private Channel fileChannel;
        private String filePath;

        ContinuesWriteDataHandler(Channel fileChannel, String path) {
            this.fileChannel = fileChannel;
            this.filePath = path;
        }

        boolean isReadyClose() {
            return readyClose.get();
        }

        boolean isComplete() {
            return writeCounter.get() == completeCounter.get();
        }

        void close() {
            readyClose.getAndSet(true);
            if (isComplete()) {
                synchronized (this) {
                    if (fileChannel != null) {
                        LOGGER.info("close file io channel ->{}", filePath);
                        closeChannel(fileChannel);
                        fileChannel = null;
                    }
                }
            }
        }

        ContinuesWriteDataHandler submitWrite() {
            writeCounter.incrementAndGet();
            return this;
        }

        public void continuousWrite(String content) {
            try {
                AsynchronousFileChannel openedChannel = (AsynchronousFileChannel) fileChannel;

                byte[] byteStream = content.getBytes(CharsetUtil.UTF_8);
                ByteBuffer buffer = ByteBuffer.wrap(byteStream);
                long position = openedChannel.size();
                buffer.position(byteStream.length);
                buffer.flip();

                openedChannel.write(buffer, position, openedChannel, this.submitWrite());
            } catch (Exception e) {
                LOGGER.error("持续写入文件出错", e);
            }
        }

        public void closeFileChannel() {
            close();
        }

        @Override
        public void completed(Integer result, Channel attachment) {
            completeCounter.incrementAndGet();
            if (isReadyClose()) {
                close();
            }
        }

        @Override
        public void failed(Throwable exc, Channel attachment) {
            LOGGER.error("写入出错", exc);
            completeCounter.incrementAndGet();
            if (isReadyClose()) {
                close();
            }
        }
    }


}

