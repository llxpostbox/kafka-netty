package kafka.netty.consumer.client;

import com.alibaba.fastjson.JSONObject;

import kafka.netty.consumer.entity.ClientRequest;
import kafka.netty.consumer.entity.User;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NettyClientApplication {
    /**
     * 模拟用户并发请求
     */
    static  class UserRequestThread implements Runnable{
        private User user;
        private TcpNettyClient tcpNettyClient;
        public UserRequestThread(User user){
            this.user = user;
            this.tcpNettyClient = new TcpNettyClient();
        }

        public void run() {

            ClientRequest request = new ClientRequest();
            request.setCommand("receive"); // 必须为   RECEIVE 或 receive
            request.setContent(user);
            //拿到请求的结果
            Object result = tcpNettyClient.send(request);
            JSONObject json = JSONObject.parseObject(JSONObject.toJSONString(result));
            if(200 == Integer.parseInt(json.get("status").toString())){
                System.out.println("连接成功");
                // 发送数据接收准备请求
                request.setCommand("OK");   // 必须为 OK 或 ok
                result = tcpNettyClient.send(request);
                // TODO 数据接入业务, 数据出口在  TcpClientHandler 下的  channelRead 方法
                if(result == null){
                    System.out.println(JSONObject.toJSONString("ok -> connect success! consumer receive data running..."));
                }else{
                    System.out.println(JSONObject.toJSONString("ok -> 其他状态："+result));
                }

            } else {
                System.out.println(JSONObject.toJSONString("receive -> 其他状态："+result));
            }
        }
    }
    public static void main(String[] args) {
        ExecutorService pool = Executors.newFixedThreadPool(10);
//        for(int i=1; i<=10; i++){
            User user = new User();
            user.setName("test2");
            user.setPassword("123456");
            user.setTopic("iscas-nd-test");
            pool.execute(new UserRequestThread(user));
//        }
    }
}
