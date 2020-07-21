package iscas.kafka.data.open.platform.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import iscas.kafka.data.open.platform.netty.conf.KafkaConfig;
import iscas.kafka.data.open.platform.netty.conf.NettyServiceConfig;
import iscas.kafka.data.open.platform.netty.content.KafkaContent;

import iscas.kafka.data.open.platform.netty.content.LockContent;
import iscas.kafka.data.open.platform.netty.zk.Constants;
import iscas.kafka.data.open.platform.netty.zk.ServiceRegistry;
import iscas.kafka.data.open.platform.netty.zk.lock.ZookeeperNodeLock;
import org.apache.log4j.Logger;


import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class KafkaNettyServer {

    private static final Logger logger = Logger.getLogger(KafkaNettyServer.class);

    private static EventLoopGroup bossLoopGroup;
    private static EventLoopGroup workLoopGroup;
    private static ServerBootstrap server;
    // netty.zk.cluster
    private static ServiceRegistry serviceRegistry;
    private static String HOST;
    private static Integer PORT;
    static {
        // 服务注册
        serviceRegistry = new ServiceRegistry();
        LockContent.LEADER_VOTE_LOCK = new ZookeeperNodeLock(Constants.ZK_LOCK_PATH);
        HOST = NettyServiceConfig.nettyConfig.getProperty("netty.host");
        PORT = Integer.parseInt(NettyServiceConfig.nettyConfig.getProperty("netty.port"));
        System.out.println("server port： "+PORT);
        // netty 服务
        bossLoopGroup = new NioEventLoopGroup();
        workLoopGroup = new NioEventLoopGroup();
        server = new ServerBootstrap();
        server.group(bossLoopGroup,workLoopGroup);
        server.channel(NioServerSocketChannel.class);
        // 请求用完时，存放新来请求的缓存队列长度,默认50
        server.option(ChannelOption.SO_BACKLOG,50);
        server.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        // 心跳机制，两个小时左右上层没有任何数据传输的情况下，这套机制才会被激活
        server.option(ChannelOption.SO_KEEPALIVE,true);
        // TCP_NODELAY就是用于启用或关于Nagle算法。如果要求高实时性，有数据发送时就马上发送，就将该选项设置为true关闭Nagle算法；
        // 如果要减少发送次数减少网络交互，就设置为false等累积一定大小后再发送。默认为false
        server.option(ChannelOption.TCP_NODELAY, false);
        //注意服务端这里一定要用childHandler 不能用handler 否则会报错
        server.childHandler(new KafkaNettyServerInitalizer());
    }

    public static void run(){
        // start conf
        KafkaConfig.initKafkaConf();     // kafka 连接参数配置初始化
        KafkaContent.initKafkaContent(); // kafka 上下文初始化
//        System.out.println(KafkaContent.partitionMap);
        try {
            // netty 服务启动
            ChannelFuture future = server.bind(new InetSocketAddress(PORT)).sync();
            if(future.isSuccess()){
                logger.info(KafkaNettyServer.class + " 启动正在监听： " + future.channel().localAddress());
            }
            // netty服务注册（注册到zookeeper）
            if(serviceRegistry != null){
                // {id=1,host=localhost,port=8080}
                Map<String,Object> map = new HashMap<>();
                map.put("host",HOST);
                map.put("port",PORT);
                map.put("id",NettyServiceConfig.nettyConfig.getProperty("netty.server.id"));
                serviceRegistry.register(map);
            }
            future.channel().closeFuture().sync();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossLoopGroup.shutdownGracefully();
            workLoopGroup.shutdownGracefully();
        }
    }
}
