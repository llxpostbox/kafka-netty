package iscas.kafka.data.open.platform.netty.content;

import com.alibaba.fastjson.JSONObject;
import iscas.kafka.data.open.platform.netty.bean.User;
import iscas.kafka.data.open.platform.netty.zk.Constants;
import iscas.kafka.data.open.platform.netty.zk.ZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ActiveUserContent {
    private static final Logger logger = Logger.getLogger(ActiveUserContent.class);

    // channel  -- znode
    private static Map<String,String> channelZnodePathMap = new ConcurrentHashMap<>();
    private ZkClient zkClient;

    public ActiveUserContent(){
        this.zkClient = ZkConnection.getZkClient();
    }
    /**
     * 新增通道用户绑定
     */
    public synchronized void addChannelBound(String channelId,User user){
        try{
            // 更新配置
            updateConsumerConfig(channelId,user);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 更新配置
     */
    private void updateConsumerConfig(String channelId,User user)  {
        // 节点检测
        if(!zkClient.exists(Constants.ZK_CONSUMER_PATH)){
            zkClient.createPersistent(Constants.ZK_CONSUMER_PATH);
        }
        // 更新消费信息到zookeeper，  user-topic-channel开头的临时节点
//        for(Map.Entry<String,User> entry:channelUserMap.entrySet()){
            String path = user.getName()+"|"+user.getTopic()+"|"+channelId+"|";
            // 临时序列节点
            String znodePath = zkClient.createEphemeralSequential(Constants.ZK_CONSUMER_PATH+"/"+path
                    ,JSONObject.toJSONString(user));
            //
            channelZnodePathMap.put(channelId,znodePath);
//        }
    }

    /**
     * 下载配置消费者配置
     */
    public void downloadConsumerConfig(Map<String, User> channelUserMap, Map<String, Map<String,Integer>> topicChannelNumberMap) {
        // 节点检测
        if(zkClient.exists(Constants.ZK_CONSUMER_PATH)){
            // 节点存在  user-topic-channel
            List<String> children = zkClient.getChildren(Constants.ZK_CONSUMER_PATH);
            // 数据组装
            for(String child : children){
                // username | topic | channelId | 节点编号
                String[] childInfo = child.split("\\|",-1);
                String userStr = zkClient.readData(Constants.ZK_CONSUMER_PATH+"/"+child);
//                    logger.info("&&&&&&&&&&&&&&&&&&&&&& "+ userStr);
//                    String obj = new String(userByte);
                User user = JSONObject.parseObject(userStr,User.class);
                // 用户信息缓存
                channelUserMap.put(childInfo[2],user);
                // topic使用情况
                if (topicChannelNumberMap.get(user.getName()) == null ){
                    Map<String,Integer> topicMap = new HashMap<>();
                    topicMap.put(user.getTopic(),1);
                    topicChannelNumberMap.put(user.getName(),topicMap);
                }else{
                    Integer nowUsed = topicChannelNumberMap.get(user.getName()).get(user.getTopic());
                    if(nowUsed == null){
                        nowUsed = 0;
                    }
                    topicChannelNumberMap.get(user.getName()).put(user.getTopic(),nowUsed +1);
                }

            }
        }else{
            throw new RuntimeException("zk未发现节点"+Constants.ZK_CONSUMER_PATH);
        }
    }

    /**
     * 移除通道-用户绑定信息
     * @param channelId 通道id
     */
    public synchronized void removeChannelBound(String channelId){
        if(channelZnodePathMap.get(channelId) == null){
            return;
        }
        LockContent.LEADER_VOTE_LOCK.lock();
        try{
            // 删除节点
            if(zkClient.exists(channelZnodePathMap.get(channelId))){
                zkClient.delete(channelZnodePathMap.get(channelId));
            }
        }catch (Exception e){e.printStackTrace();}finally {
            LockContent.LEADER_VOTE_LOCK.unlock();

        }
    }
}
