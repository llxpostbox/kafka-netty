package iscas.kafka.data.open.platform.netty;

import iscas.kafka.data.open.platform.netty.conf.NettyServiceConfig;
import iscas.kafka.data.open.platform.netty.server.KafkaNettyServer;

public class ServerApplication {

    public static void main(String[] args) {
        // 关闭日志
        if(args.length == 1){
            NettyServiceConfig.confPath=args[0];
            // 初始化跑配置
            NettyServiceConfig.init();
        }
        KafkaNettyServer.run();
    }
}
