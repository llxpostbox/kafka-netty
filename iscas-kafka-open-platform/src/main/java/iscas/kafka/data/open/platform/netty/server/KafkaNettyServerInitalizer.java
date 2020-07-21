package iscas.kafka.data.open.platform.netty.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

public class KafkaNettyServerInitalizer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(new DelimiterBasedFrameDecoder(Integer.MAX_VALUE, Delimiters.lineDelimiter()[0]));
        //添加客户端和服务端之间的心跳检查状态
        ch.pipeline().addLast(new IdleStateHandler(6, 2, 1, TimeUnit.SECONDS));
        ch.pipeline().addLast(new KafkaNettyServerHandler());
        ch.pipeline().addLast(new StringEncoder());
    }
}
