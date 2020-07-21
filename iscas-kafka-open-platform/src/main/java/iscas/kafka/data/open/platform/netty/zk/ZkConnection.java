package iscas.kafka.data.open.platform.netty.zk;

import iscas.kafka.data.open.platform.netty.conf.NettyServiceConfig;
import org.I0Itec.zkclient.ZkClient;

public class ZkConnection {

    public static ZkClient getZkClient(){
        ZkClient zkClient = null;
        try {
            // zookeeper 地址检查
            if(NettyServiceConfig.nettyConfig.getProperty("netty.zk.cluster") == null ){
                NettyServiceConfig.init();
            }
            zkClient = new ZkClient(NettyServiceConfig.nettyConfig.getProperty("netty.zk.cluster"),Constants.ZK_SESSION_TIMEOUT);
            zkClient.setZkSerializer( new MyZkSerializer());
            //
        }catch (Exception e){
            e.printStackTrace();
        }
        return zkClient;
    }

    /**
     * 关闭 zk 客户端
     * @param zkClient  zk客户端
     */
    public static void closeZk(ZkClient zkClient){
        if(zkClient != null){
            zkClient.close();
        }
    }
}
