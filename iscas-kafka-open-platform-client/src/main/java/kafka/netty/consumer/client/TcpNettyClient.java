package kafka.netty.consumer.client;

import com.alibaba.fastjson.JSONObject;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import kafka.netty.consumer.entity.ClientRequest;
import kafka.netty.consumer.entity.DefaultFuture;
import kafka.netty.consumer.entity.Response;



public class TcpNettyClient {
    private EventLoopGroup group =null;
    private Bootstrap client =null;
    private  ChannelFuture future=null;

    public void closeClient(){
        //关闭通道
        future.channel().close();
    }
    public TcpNettyClient(){
        group = new NioEventLoopGroup();
        client = new Bootstrap();
        client.group(group);
        client.channel(NioSocketChannel.class);
        client.option(ChannelOption.SO_KEEPALIVE,true);
        client.handler(new TcpClientInitalizer());
        try {
//            future = client.connect("localhost", 8080).sync();
            future = client.connect("localhost", 8080);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //发送数据的方法
    public Object send(ClientRequest request){
        try{
            System.out.println(Thread.currentThread().getName()+" request: "+ JSONObject.toJSONString(request));
            //客户端直接发送请求数据到服务端
            future.channel().writeAndFlush(JSONObject.toJSONString(request));
            //根据\r\n进行换行
            future.channel().writeAndFlush("\r\n");
//            System.out.println("********************** "+ future.channel().id());
            //通过请求实例化请求和响应之间的关系
            DefaultFuture defaultFuture = new DefaultFuture(request);
            //通过请求ID，获取对应的响应处理结果
            Response response = defaultFuture.get(10);
            return response;
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

}
