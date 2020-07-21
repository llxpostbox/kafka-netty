package iscas.kafka.data.open.platform.netty.zk;

import com.google.gson.Gson;
import iscas.kafka.data.open.platform.netty.content.KafkaContent;
import iscas.kafka.data.open.platform.netty.content.LockContent;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

public class NettyLeaderChangeTimerTask extends TimerTask {
    private static final Logger logger = Logger.getLogger(NettyLeaderChangeTimerTask.class);

    private ZkClient zkClient;
    private int serverId;

    public NettyLeaderChangeTimerTask(ZkClient zkClient, int serverId) {
        this.zkClient = zkClient;
        this.serverId = serverId;
    }

    @Override
    public void run() {
        try {
            // 服务变动监控
            leaderChangeListener();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * leader 发送变化时操作
     */
    private void leaderChangeListener() {
        // 获取leader服务id
        String leaderData = zkClient.readData(Constants.ZK_LEADER_PATH);
        int nowServerLeaderId = -1;
        try{
            // 获取leader id
            nowServerLeaderId = Integer.parseInt(leaderData);
        }catch (Exception e){e.printStackTrace();}
        // leader service id checkout
        // leader 服务检测到zookeeper上的配置与kafka获取的配置不一样时
        // 锁住集群(锁的对象： 集群leader选举，kafka配置更新，用户变动更新)，然后更新zookeeper上的kafka配置
        if(nowServerLeaderId == serverId){ // 当前服务是leader服务， 由当前服务来同步配置信息
            // 1、获取zk的config节点配置数据(kafka分析信息)
            String nowPartitionInfo = zkClient.readData(Constants.ZK_CONFIG_PATH);
            Map<String,Object> partitionInfoMap = new HashMap<>();
            // gson 转换结束后int变成doble
            Gson gson = new Gson();
            try{
                partitionInfoMap = gson.fromJson(nowPartitionInfo, partitionInfoMap.getClass());
            }catch (Exception e){e.printStackTrace();}
            // 2、获取当前运行kafka最新的topic配置信息,先初始化一下最新配置
            KafkaContent.initKafkaContent();
            Map<String, Integer> partitionMap = KafkaContent.partitionMap;
            // 3、验证配置是否相同。相同则跳过，不相同则更新zk的config节点的数据
            if(!partitionCheckout(partitionInfoMap,partitionMap)){
                logger.info("配置发生变动");
                logger.info("before: " +partitionInfoMap);
                // leader 服务不在线， 获取分布式阻塞锁
                LockContent.LEADER_VOTE_LOCK.lock();
                try{
                    // 配置不一样， 更新zk上的配置
                    zkClient.writeData(Constants.ZK_CONFIG_PATH,partitionMap.toString());
                    logger.info("after: "+partitionMap);
                }catch (Exception e){e.printStackTrace();}finally {
                    LockContent.LEADER_VOTE_LOCK.unlock();
                }
            }
        }
    }

    /**
     * 检测kafka配置 与 zk下存储的kafka配置是否一样
     *      -- 一样返回 true
     *      -- 不一样返回 false
     * @param partitionInfoMap  zk上的配置信息
     * @param partitionMap 本地获取的kafka最新配置信息
     * @return boolean  默认相同
     */
    private boolean partitionCheckout(Map<String, Object> partitionInfoMap, Map<String, Integer> partitionMap) {
        // 长度检测
        if(partitionInfoMap.size() != partitionMap.size()){
            return false;
        }
        // key  value 检测
        for(Map.Entry<String, Integer> entry : partitionMap.entrySet()){
            String key = entry.getKey();
            Integer value = entry.getValue();
            // zk 配置
            Object obj = partitionInfoMap.get(key);
            if(obj == null && value != null){
                return false;
            }
            //
            Double zkDoubleV = null;
            try {
                assert obj != null;
                zkDoubleV = Double.parseDouble(obj.toString());
            }catch (Exception e){e.printStackTrace();}
            // 配置不一样
            if(zkDoubleV == null){
                return false;
            }
            //
            int zkValue = (int)Math.round(zkDoubleV);
            if(zkValue != value){ // 数据不相同
                return false;
            }
        }
        return true;
    }


}
