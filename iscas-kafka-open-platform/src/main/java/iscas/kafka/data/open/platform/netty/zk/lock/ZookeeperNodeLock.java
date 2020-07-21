package iscas.kafka.data.open.platform.netty.zk.lock;

import iscas.kafka.data.open.platform.netty.conf.NettyServiceConfig;
import iscas.kafka.data.open.platform.netty.zk.ZkConnection;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 分布式锁
 *    使用注意事项
 *      -- 1、不能出现锁中锁, 理解为一次锁代锁住表一个线程空间，在锁中继续使用锁会导致线程空间
 *      被第二个锁占用（替换），第二个锁释放后。 回到第一个锁进行释放时锁的线程空间为空了，会报错：
 *      java.lang.IllegalArgumentException: Path cannot be null
 */
public class ZookeeperNodeLock implements Lock {

    private String znode;
    private ZkClient zkClient;
    private ThreadLocal<String> currentNode = new ThreadLocal<>(); //当前节点
    private ThreadLocal<String> beforeNode = new ThreadLocal<>();  //前一个节点

    public ZookeeperNodeLock(String znode) {
        // 初始化跑配置
        NettyServiceConfig.init();
        // 节点检查
        if(StringUtils.isBlank(znode)) {
            throw new IllegalArgumentException("锁节点znode不能为空字符串");
        }
        this.znode = znode;
//        this.zkClient = new ZkClient(NettyServiceConfig.nettyConfig.getProperty("netty.zk.cluster"));
        this.zkClient = ZkConnection.getZkClient();
        try {
            if(!this.zkClient.exists(znode)) {
                this.zkClient.createPersistent(znode, true); // true是否创建层级目录
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lock() {
        if(!tryLock()) {
            waitLock();
            lock();
        }
    }

    private void waitLock() {
        CountDownLatch latch = new CountDownLatch(1);
        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataDeleted(String dataPath) {
//                System.out.println("{"+dataPath+"}节点删除，锁释放... ");
                latch.countDown();
            }

            @Override
            public void handleDataChange(String dataPath, Object data) {
            }
        };
        this.zkClient.subscribeDataChanges(beforeNode.get(), listener);

        try {
            if(this.zkClient.exists(beforeNode.get())) {
                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.zkClient.unsubscribeDataChanges(beforeNode.get(), listener);
    }

    @Override
    public boolean tryLock() {
        boolean result = false;
        // 创建顺序临时节点
        if(null == currentNode.get() || !this.zkClient.exists(currentNode.get())) {
            String enode = this.zkClient.createEphemeralSequential(znode + "/", "zk-locked");
            this.currentNode.set(enode);
        }
        // 获取znode节点下的所有子节点
        List<String> list = this.zkClient.getChildren(znode);
        Collections.sort(list);

        if(currentNode.get().equals(this.znode + "/" + list.get(0))) {
//            log.info("{}节点为头结点，获得锁...", currentNode.get());
//            System.out.println("{"+currentNode.get()+"}节点为头结点，获得锁...");
            result = true;
        } else {
            int currentIndex = list.indexOf(currentNode.get().substring(this.znode.length() + 1));
            String bnode = this.znode + "/" + list.get(currentIndex - 1);
            this.beforeNode.set(bnode);
            System.out.println(beforeNode.get());
        }
        return result;
    }

    @Override
    public void unlock() {
        // 锁中使用锁时 currentNode.get() 可能为空
        if(null != this.currentNode && currentNode.get() != null) {
            this.zkClient.delete(currentNode.get());
            this.currentNode.set(null);
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)  {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void lockInterruptibly() {
        // TODO Auto-generated method stub
    }

    @Override
    public Condition newCondition() {
        // TODO Auto-generated method stub
        return null;
    }

}
