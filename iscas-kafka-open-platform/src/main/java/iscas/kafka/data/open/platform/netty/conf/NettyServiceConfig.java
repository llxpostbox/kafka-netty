package iscas.kafka.data.open.platform.netty.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class NettyServiceConfig {

    public static Properties nettyConfig;
    public static String confPath;

    private static final Logger logger = LoggerFactory.getLogger(NettyServiceConfig.class);
    static {
        nettyConfig = new Properties();
    }
    public static void init(){
        try {
            System.out.println("配置目录： "+confPath);
            if(confPath == null){
                nettyConfig.load(NettyServiceConfig.class.getResourceAsStream("/netty.properties"));
            }else{
                nettyConfig.load(new FileInputStream(confPath+"/netty.properties"));
            }
            try{
                // 参数检查
                if(Integer.parseInt(nettyConfig.getProperty("netty.server.id")) <=0){
                    // 服务id配置错误
                    System.out.println("服务id配置错误,，服务id必须为大于0的正整数");
                    System.exit(1);
                }
            }catch (Exception e){e.printStackTrace();}
            logger.info("config init success！！");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("netty config load failed, server stopping ...");
            System.exit(1);
        }
    }
}
