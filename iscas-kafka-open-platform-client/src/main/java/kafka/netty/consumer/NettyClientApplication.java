package kafka.netty.consumer;

import com.alibaba.fastjson.JSONObject;

import kafka.netty.consumer.client.KafkaNettyClient;
import kafka.netty.consumer.conf.ClientConfig;
import kafka.netty.consumer.core.ConnectStringParser;
import kafka.netty.consumer.core.ServiceElection;
import kafka.netty.consumer.entity.ClientRequest;
import kafka.netty.consumer.entity.Response;
import kafka.netty.consumer.entity.User;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NettyClientApplication {
    public static void main(String[] args) throws IOException {
        // 数据接入
        NettyClientApplication nettyClientApplication = new NettyClientApplication();
        // 分区偏移 map
        Map<Integer,Long> map = new HashMap<>();
        // TODO 填写实际分区的实际偏移量
//        map.put(0,0L);
//        map.put(1,0L);
//        map.put(2,0L);
//        map.put(3,0L);
//        map.put(4,0L);
//        map.put(5,0L);
//        map.put(6,0L);
        nettyClientApplication.receiveAction(map);
    }

    public void receiveAction(Map<Integer,Long> offsetMap){
        ConnectStringParser connectStringParser = new ConnectStringParser(ClientConfig.conf.getProperty("kafka.netty.server.cluster"));
        ServiceElection se = new ServiceElection();
        InetSocketAddress inetSocketAddress = se.serviceElection(connectStringParser.getServerAddress());
        String host = inetSocketAddress.getHostName();
        int port = inetSocketAddress.getPort();

        User baseUser = new User(ClientConfig.conf.getProperty("kafka.netty.user"),
                ClientConfig.conf.getProperty("kafka.netty.password"),
                ClientConfig.conf.getProperty("kafka.netty.topic"));
        // netty 客户端
        KafkaNettyClient kafkaNettyClient = new KafkaNettyClient(host,port);
//        KafkaNettyClient kafkaNettyClient = new KafkaNettyClient(host,port);
        ClientRequest request = new ClientRequest();
        request.setCommand("handshake"); // 必须为   handshake
        request.setContent(baseUser);
        //拿到请求的结果
        Object result = kafkaNettyClient.send(request);
        JSONObject json = JSONObject.parseObject(JSONObject.toJSONString(result));
        if(200 == Integer.parseInt(json.get("status").toString())){
            // 返回内容转换
            User serverResponseUser = JSONObject.parseObject(json.get("content").toString(),User.class);
            System.out.println("分区数为："+serverResponseUser.getTotalPartition());
            // 创建线程池
            ExecutorService pool = Executors.newFixedThreadPool(serverResponseUser.getTotalPartition());
            for(int i=0; i<serverResponseUser.getTotalPartition(); i++){
                User user = new User(baseUser.getName(),baseUser.getPassword(),baseUser.getTopic());
                user.setUsedPartition(i);
                user.setOffset(offsetMap.get(i));
                pool.execute(new UserRequestThread(user,host,port));
            }
        }else{
            System.out.println(json);
        }
    }
    /**
     * 模拟用户并发请求
     */
    static  class UserRequestThread implements Runnable{
        private User user;
        private KafkaNettyClient kafkaNettyClient;

        public UserRequestThread(User user,String host, int port){
            this.user = user;
            this.kafkaNettyClient = new KafkaNettyClient(host,port);
        }
        public void run() {
            // connectionServer 线程启动连接
            boolean connSuccess = connectionServer();
            // 连接失败次数统计
            int failedCount = 0;
            // 连接检测,连接失败重连
            while(!connSuccess){
                if(failedCount >= 5){
                    System.out.println("分区[ "+user.getUsedPartition()+" ]连接失败{"+failedCount+"}次,终止连接.");
                    break;
                }
                failedCount ++;
                System.out.println(Thread.currentThread().getName()+" --> 分区[ "+user.getUsedPartition()+" ]连接失败次数: "+failedCount+" --> 3 秒后重新连接。");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // 继续连接
                connSuccess = connectionServer();
            }
        }

        private boolean connectionServer() {
            boolean conn = false;
            ClientRequest request = new ClientRequest();
            request.setCommand("checkout"); // 必须为   CHECKOUT 或 checkout
            request.setContent(user);
            //拿到请求的结果
            Object result = kafkaNettyClient.send(request);
            JSONObject json = JSONObject.parseObject(JSONObject.toJSONString(result));
            if(json != null && 200 == Integer.parseInt(json.get("status").toString())){
                System.out.println(Thread.currentThread().getName()+" --> "+result);
                // 发送数据接收准备请求
                request.setCommand("receive");   // 必须为 RECEIVE 或 receive
                User user = JSONObject.parseObject(json.get("content").toString(),User.class);
                request.setContent(user);
                result = kafkaNettyClient.send(request);
                // 数据接入业务, 数据出口在  KafkaNettyClientHandler 下的  channelRead 方法
                if(result == null ){
                    System.out.println("数据接收请求失败");
                    conn = false;
                }else{
                    Response response = (Response)result;
                    System.out.println("------------------------------"+response);
                    System.out.println(response.getMsg());
                    conn = true;
                }
            } else {
                if(result != null){
                    Response response = (Response)result;
                    if(response.getStatus() == 202){
                        System.out.println(response.getMsg());
                        conn = true;
                    }else{
                        System.out.println(result);
                    }
                } else{
                    System.out.println("严重请求失败");
                    conn = false;
                }
            }
            return conn;
        }
    }
}
