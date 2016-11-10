package com.vsct.dt.nsq.netty;

import com.vsct.dt.nsq.Connection;
import com.vsct.dt.nsq.frames.NSQFrame;
import com.vsct.dt.nsq.frames.ResponseFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;


public class NSQHandler extends SimpleChannelInboundHandler<NSQFrame> {
    private final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ResponseFrame.class);

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        Connection connection = ctx.channel().attr(Connection.STATE).get();
        if (connection != null) {
            LOGGER.info("Channel disconnected! {} ", connection);
        } else {
            LOGGER.error("No connection set for : {}", ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        LOGGER.error("NSQHandler exception caught", cause);

        ctx.channel().close();
        Connection con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            con.close();
        } else {
            LOGGER.warn("No connection set for : " + ctx.channel());
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NSQFrame msg) throws Exception {
        final Connection con = ctx.channel().attr(Connection.STATE).get();
        if (con != null) {
            ctx.channel().eventLoop().execute(() -> con.incoming(msg));
        } else {
            LOGGER.warn("No connection set for : {}", ctx.channel());
        }
    }
}
