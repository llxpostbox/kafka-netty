package iscas.kafka.data.open.platform.netty.zk;

import iscas.kafka.data.open.platform.netty.content.KafkaContent;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;

public class ServiceRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);
    private ZkClient zkClient;

    public ServiceRegistry() {
        this.zkClient = ZkConnection.getZkClient();
    }

    /**
     * 更具服务器 {id=1,host=localhost,port=8080} 注册到zk
     * @param datamap   {id=1,host=localhost,port=8080}
     */
    public void register(Map<String,Object> datamap) {
        if (datamap != null && datamap.size() > 0) {
            // 初始化节点
            startCreateNode(datamap);
            if(zkClient == null){
                this.zkClient = ZkConnection.getZkClient();
            }
            // 服务变动监控
            Timer server = new Timer();
            NettyServerChangeTimerTask servertask = new NettyServerChangeTimerTask(zkClient,Integer.parseInt(datamap.get("id").toString()));
            server.schedule(servertask,1000, 3000);
            // 配置变动监控
            Timer leader = new Timer();
            NettyLeaderChangeTimerTask leadertask = new NettyLeaderChangeTimerTask(zkClient,Integer.parseInt(datamap.get("id").toString()));
            leader.schedule(leadertask,1000,10000);
        }
    }

    /**
     * 创建节点
     * @param dataMap   {id=1,host=localhost,port=8080}
     */
    private void startCreateNode(Map<String,Object> dataMap) {
        try {
            // 服务类型节点创建
            if(!zkClient.exists(Constants.ZK_DATA_PATH)){
                zkClient.createPersistent(Constants.ZK_DATA_PATH);
            }
            // 服务节点创建
            if(!zkClient.exists(Constants.ZK_SERVER_PATH)){
                zkClient.createPersistent(Constants.ZK_SERVER_PATH);
            }
            // 配置节点创建
            if (!zkClient.exists(Constants.ZK_CONFIG_PATH)) {
                zkClient.createPersistent(Constants.ZK_CONFIG_PATH);
            }
            // 主节点创建
            if(!zkClient.exists(Constants.ZK_LEADER_PATH)){
                zkClient.createPersistent(Constants.ZK_LEADER_PATH);
            }
            // 消费者信息节点
            if(!zkClient.exists(Constants.ZK_CONSUMER_PATH)){
                zkClient.createPersistent(Constants.ZK_CONSUMER_PATH);
            }
            // 注册服务  -- 临时节点
            String path = zkClient.createEphemeralSequential(Constants.ZK_SERVER_PATH+"/service"
                    , dataMap.toString()
                    , ZooDefs.Ids.OPEN_ACL_UNSAFE);
            System.out.println("server register path： "+path);
            // 设置配置数据
            zkClient.writeData(Constants.ZK_CONFIG_PATH,KafkaContent.partitionMap);
            // 设置leader,启动时：谁先启动也谁为leader
            String leaderId = zkClient.readData(Constants.ZK_LEADER_PATH);
            if(leaderId == null){
                // leader 节点没有选举,也当前服务节点为领导节点
                zkClient.writeData(Constants.ZK_LEADER_PATH,dataMap.get("id").toString());
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }
}
