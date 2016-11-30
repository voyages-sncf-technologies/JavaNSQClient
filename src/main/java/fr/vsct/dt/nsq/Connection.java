package fr.vsct.dt.nsq;

import fr.vsct.dt.nsq.callbacks.NSQErrorCallback;
import fr.vsct.dt.nsq.exceptions.NSQException;
import fr.vsct.dt.nsq.exceptions.NoConnectionsException;
import fr.vsct.dt.nsq.frames.ErrorFrame;
import fr.vsct.dt.nsq.frames.MessageFrame;
import fr.vsct.dt.nsq.frames.NSQFrame;
import fr.vsct.dt.nsq.frames.ResponseFrame;
import fr.vsct.dt.nsq.netty.NSQClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Connection {
    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();
    public static final AttributeKey<Connection> STATE =
            AttributeKey.valueOf("Connection.state");
    private final ServerAddress address;
    private final Channel channel;
    private NSQConsumer consumer = null;
    private NSQErrorCallback errorCallback = null;
    private final LinkedBlockingQueue<NSQCommand> requests = new LinkedBlockingQueue<>(1);
    private final LinkedBlockingQueue<NSQFrame> responses = new LinkedBlockingQueue<>(1);
    private static EventLoopGroup defaultGroup = null;
    private final EventLoopGroup eventLoopGroup;
    private final NSQConfig config;

    private final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Connection.class);

    public Connection(final ServerAddress serverAddress, final NSQConfig config) throws NoConnectionsException {
        this.address = serverAddress;
        this.config = config;
        final Bootstrap bootstrap = new Bootstrap();
        eventLoopGroup = config.getEventLoopGroup() != null ? config.getEventLoopGroup() : getDefaultGroup();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NSQClientInitializer());
        // Start the connection attempt.
        final ChannelFuture future = bootstrap.connect(new InetSocketAddress(serverAddress.getHost(),
                serverAddress.getPort()));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        LOGGER.info("Created connection: {}", serverAddress);
        this.channel.attr(STATE).set(this);
        final ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(MAGIC_PROTOCOL_VERSION);
        channel.write(buf);
        channel.flush();

        //indentify
        final NSQCommand ident = NSQCommand.instance("IDENTIFY", config.toString().getBytes());
        try {
            final NSQFrame response = commandAndWait(ident);
            if (response != null) {
                LOGGER.info("Server identification: {}", ((ResponseFrame) response).getMessage());
            }
        } catch (final TimeoutException e) {
            LOGGER.error("Creating connection timed out", e);
            close();
        }
    }

    private EventLoopGroup getDefaultGroup() {
        if (defaultGroup == null) {
            defaultGroup = new NioEventLoopGroup();
        }
        return defaultGroup;
    }

    public boolean isConnected() {
        return channel.isActive();
    }

    public boolean isRequestInProgress() {
        return requests.size() > 0;
    }

    public void incoming(final NSQFrame frame) {
        if (frame instanceof ResponseFrame) {
            if ("_heartbeat_".equals(((ResponseFrame) frame).getMessage())) {
                heartbeat();
                return;
            } else {
                if (!requests.isEmpty()) {
                    try {
                        responses.offer(frame, 20, TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                        LOGGER.error("Thread was interruped, probably shuthing down", e);
                        close();
                    }
                }
                return;
            }
        }

        if (frame instanceof ErrorFrame) {
            if (errorCallback != null) {
                errorCallback.error(NSQException.of((ErrorFrame) frame));
            }
            responses.add(frame);
            return;
        }

        if (frame instanceof MessageFrame) {
            final MessageFrame msg = (MessageFrame) frame;

            final NSQMessage message = new NSQMessage();
            message.setAttempts(msg.getAttempts());
            message.setConnection(this);
            message.setId(msg.getMessageId());
            message.setMessage(msg.getMessageBody());
            message.setTimestamp(new Date(TimeUnit.NANOSECONDS.toMillis(msg.getTimestamp())));
            consumer.processMessage(message);
            return;
        }
        LOGGER.warn("Unknown frame type: {}", frame);
    }


    private void heartbeat() {
        LOGGER.trace("HEARTBEAT!");
        command(NSQCommand.instance("NOP"));
    }

    public void setErrorCallback(final NSQErrorCallback callback) {
        errorCallback = callback;
    }

    public void close() {
        LOGGER.info("Closing  connection: {}", this);
        channel.disconnect();
    }

    public NSQFrame commandAndWait(final NSQCommand command) throws TimeoutException {
        try {
            if (!requests.offer(command, 15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            responses.clear(); //clear the response queue if needed.
            final ChannelFuture fut = command(command);

            if (!fut.await(15, TimeUnit.SECONDS)) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            final NSQFrame frame = responses.poll(15, TimeUnit.SECONDS);
            if (frame == null) {
                throw new TimeoutException("command: " + command + " timedout");
            }

            requests.poll(); //clear the request object
            return frame;
        } catch (final InterruptedException e) {
            close();
            LOGGER.warn("Thread was interruped!", e);
        }
        return null;
    }

    public ChannelFuture command(final NSQCommand command) {
        return channel.writeAndFlush(command);
    }

    public ServerAddress getServerAddress() {
        return address;
    }

    public NSQConfig getConfig() {
        return config;
    }

    public void setConsumer(final NSQConsumer consumer) {
        this.consumer = consumer;
    }
}
