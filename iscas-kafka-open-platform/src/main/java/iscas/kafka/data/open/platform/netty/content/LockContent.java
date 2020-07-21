package iscas.kafka.data.open.platform.netty.content;


import iscas.kafka.data.open.platform.netty.zk.lock.ZookeeperNodeLock;

public class LockContent {
    /**
     * 分布式锁服务选择锁
     */
    public static ZookeeperNodeLock LEADER_VOTE_LOCK;
}
