package kafka.netty.consumer.core;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.ChannelOption;
import kafka.netty.consumer.client.KafkaNettyClientInitalizer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ServiceElection {
    private static LoadStrategy loadStrategy = LoadStrategy.RANDOM;

    public ServiceElection() {
        this(loadStrategy);
    }
    public ServiceElection(LoadStrategy loadStrategy) {
        this.loadStrategy = loadStrategy;
    }
    /**
     *  service election
     * @param serverAddress
     */
    public InetSocketAddress serviceElection(ArrayList<InetSocketAddress> serverAddress) {
        InetSocketAddress inetSocketAddress = null;
        switch (loadStrategy){
            // 完全随机
            case RANDOM: inetSocketAddress =  fullRandomElection(serverAddress); break;
            // 轮询
            case POLL: inetSocketAddress = pollElection(serverAddress); break;
            // 默认
            default: break;
        }
        return inetSocketAddress;
    }

    /**
     * 轮询选取
     * @param serverAddress
     */
    private InetSocketAddress pollElection(ArrayList<InetSocketAddress> serverAddress) {
        // 未实现 -----------采用完全随机选取
        loadStrategy = LoadStrategy.RANDOM;
        return fullRandomElection(serverAddress);

    }

    /**
     * 完全随机选取
     * @param serverAddress 分为地址
     */
    private InetSocketAddress fullRandomElection(ArrayList<InetSocketAddress> serverAddress){
        Map<Integer,InetSocketAddress> map = new HashMap<>();
        int key=0;
        for (InetSocketAddress address : serverAddress){
            map.put(key,address);
            key ++;
        }
        Random random = new Random();
        int index = random.nextInt(serverAddress.size());
        // 被选中的服务信息
        InetSocketAddress inetSocketAddress = serverAddress.get(index);
        // 服务检查
        if(serverAlive(inetSocketAddress)){
            System.out.println("选中服务: "+inetSocketAddress.getHostName()+":"+inetSocketAddress.getPort());
            return inetSocketAddress;
        }else{
            map.remove(index);
            if(map.size() == 0){
                // 服务器连接失败
                System.out.println("服务器连接失败");
                System.exit(1);
            }
            ArrayList<InetSocketAddress> server = new ArrayList<>();
            for (Map.Entry<Integer,InetSocketAddress> entry : map.entrySet()){
                server.add(entry.getValue());
            }
            fullRandomElection(server);
        }
        return null;
    }

    /**
     * 服务存活检查
     * @param inetSocketAddress 服务地址
     * @return 默认 fasle
     */
    private boolean serverAlive(InetSocketAddress inetSocketAddress) {
        boolean serverAlive= false;
        NioEventLoopGroup group = null;
        try{
            //
            group = new NioEventLoopGroup();
            Bootstrap client = new Bootstrap();
            client.group(group);
            client.channel(NioSocketChannel.class);
            client.option(ChannelOption.SO_KEEPALIVE,true);
            client.handler(new KafkaNettyClientInitalizer());
            client.connect(inetSocketAddress.getHostName(), inetSocketAddress.getPort()).sync();
            serverAlive = true;
        }catch (Exception e){
            e.printStackTrace();
        } finally {
            if(group != null){
                System.out.println("关闭连接测试");
                group.shutdownGracefully();
            }

        }
        return serverAlive;
    }
}
