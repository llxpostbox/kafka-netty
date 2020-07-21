package kafka.netty.consumer.client;

import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import kafka.netty.consumer.entity.DefaultFuture;
import kafka.netty.consumer.entity.Response;


public class TcpClientHandler extends ChannelInboundHandlerAdapter {
    private long sum = 0;
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        //判断服务端和客户端是在能够正常通信的情况下
        if(msg.toString().equals("ping")){
            ctx.channel().writeAndFlush("ping\r\n");
            return ;
        }

//        System.out.println(Thread.currentThread().getName()+" >>> 获取到服务端响应数据:"+msg.toString());
        System.out.println("netty 消费 > " +Thread.currentThread().getName()+" >>> 获取到服务端响应数据: "+sum + "条");
        sum ++;
        String str = getJSONObject(msg.toString()).toString();
        //读取服务端的响应结果
        Response res = JSONObject.parseObject(str, Response.class);
        //存储响应结果
        DefaultFuture.recive(res);
    }

    private JSONObject getJSONObject(String str){
        // 去掉多余信息
        JSONObject json = JSONObject.parseObject(str);
        json.remove("content");
        return json;
    }
}
