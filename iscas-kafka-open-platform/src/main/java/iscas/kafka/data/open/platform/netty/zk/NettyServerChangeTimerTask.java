package iscas.kafka.data.open.platform.netty.zk;

import com.google.gson.Gson;
import iscas.kafka.data.open.platform.netty.conf.NettyServiceConfig;
import iscas.kafka.data.open.platform.netty.content.LockContent;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooDefs;

import java.util.*;

/**
 * netty 服务监听任务
 */
public class NettyServerChangeTimerTask extends TimerTask {
    private static final Logger logger = Logger.getLogger(NettyServerChangeTimerTask.class);

    private ZkClient zkClient;
    private int serverId;

    public NettyServerChangeTimerTask(ZkClient zkClient,int serverId){
        this.zkClient = zkClient;
        this.serverId = serverId;
    }

    @Override
    public void run() {

        try {
            // 服务变动监控
            serverChangeListener();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 服务变动监听
     *  -- 监听内容
     *     -- 1、leader 数据错误时重新选择leader
     *     -- 2、服务节点上下线变动监听
     *     -- 3、leader选举
     *         -- 选举规则：
     *             -- 检测到leader下线后触发选举，停止接收新的数据请求（注：不影响原有正常连接）
     *             -- 有存活的最大服务id的服务来选举（顺应继承）
     *             -- 阻塞3秒、选举存活服务中服务id最大的服务节点为新的leader节点
     *             -- 开放服务
     *     -- 4、 数据同步（注：配置数据同步）
     *
     */
    private void serverChangeListener() {
        // 获取leader节点信息
        String leaderData = zkClient.readData(Constants.ZK_LEADER_PATH);
        int nowServerLeaderId = -1;
        try{
            // 获取leader id
            nowServerLeaderId = Integer.parseInt(leaderData);
        }catch (Exception e){e.printStackTrace();}
//        System.out.println("leader: "+nowServerLeaderId);
        // 服务获取
        List<String> serverList = zkClient.getChildren(Constants.ZK_SERVER_PATH);
        // leader 在线, 默认leader已经死了
        boolean leaderAlive = false;
        // 当前服务也注册
        boolean nowServerAlive = false;

        int maxServerId = -1;
        Set<Integer> allServer = new HashSet<>();
        for(String server : serverList){
            String nowServerInfo = zkClient.readData(Constants.ZK_SERVER_PATH+"/"+server);
            Map serverInfoMap = new HashMap<>();
            Gson gson = new Gson();
            serverInfoMap = gson.fromJson(nowServerInfo, serverInfoMap.getClass());
//            System.out.println(server+" --> "+serverInfoMap);
            try{
                int nowServerId = Integer.parseInt(serverInfoMap.get("id").toString().substring(0,1));
                allServer.add(nowServerId);
                if(nowServerId == nowServerLeaderId){ // 检查到leader节点的服务还活着
                    leaderAlive = true;
                }
                // 能在服务列表中找到与当前服务id相同的服务,表示当前服务正常， 否则可能是zk连接超时而将服务下线， 但是服务确是正常的
                if(nowServerId == serverId){
                    nowServerAlive = true;
                }
                if(nowServerId > maxServerId){  // 记录存活服务最大id
                    maxServerId = nowServerId;
                }
            }catch (Exception e){e.printStackTrace();}
        }
        logger.info("nowLeader: "+nowServerLeaderId +" | leader alive: "+leaderAlive+" | serverId: "+serverId+" | maxServerId: "+maxServerId+" | allServer: "+allServer);
        // 当前服务活跃检测
        if (!nowServerAlive){
            // 当前服务也被zk下线， 但是服务正常活着， 重新注册到zk
            logger.info("当前服务[ "+serverId+" ]存活，zk服务列表"+allServer+","+serverId+ "服务重新注册......");
            // 注册服务到zookeeper
            Map<String,Object> dataMap = new HashMap<>();
            dataMap.put("host",NettyServiceConfig.nettyConfig.getProperty("netty.host"));
            dataMap.put("port",Integer.parseInt(NettyServiceConfig.nettyConfig.getProperty("netty.port")));
            dataMap.put("id", serverId);
            // 注册服务  -- 临时节点
            String path = zkClient.createEphemeralSequential(Constants.ZK_SERVER_PATH+"/service"
                    , dataMap.toString()
                    , ZooDefs.Ids.OPEN_ACL_UNSAFE);
            logger.info("server register success, path： "+path);
        }
        // 检查leader是否活着
        // leader 选举规则
        if(!leaderAlive && maxServerId == serverId){  // leader 死了,
            // leader 服务不在线， 获取分布式阻塞锁
            LockContent.LEADER_VOTE_LOCK.lock();
            try{
                // 重新选举leader
                if(maxServerId > 0){
                    zkClient.writeData(Constants.ZK_LEADER_PATH,String.valueOf(maxServerId));
                    // 新leader选择成功后
                    logger.info("leader 已经重新选举， 新服务 id = "+maxServerId);
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                LockContent.LEADER_VOTE_LOCK.unlock();
            }
        }
    }
}
