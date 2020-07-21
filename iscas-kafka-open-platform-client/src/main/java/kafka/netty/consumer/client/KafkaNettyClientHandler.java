package kafka.netty.consumer.client;

import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import kafka.netty.consumer.entity.DefaultFuture;
import kafka.netty.consumer.entity.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;


public class KafkaNettyClientHandler extends ChannelInboundHandlerAdapter {

    private static Map<String,Long> countMap = new HashMap<>();

    // 创建定时任务
    private static Timer timer = new Timer();
    static {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                for(Map.Entry<String,Long> entry : countMap.entrySet()){
                    System.out.println(entry.getKey()+" 分区消费数据： "+entry.getValue()+" 条");
                }
                System.out.println("--------------------------------------");
                System.out.println(Thread.currentThread().getName());
                System.out.println("**************************************");
            }
        },1000,5000);
    }
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println(msg);
        Response response = JSONObject.parseObject(msg.toString(),Response.class);
        if(response.getId() != null && response.getId() >=0){
            String str = getJSONObject(msg.toString()).toString();
            //读取服务端的响应结果
            Response res = JSONObject.parseObject(str, Response.class);
            //存储响应结果
            DefaultFuture.recive(res);
        }else{
            // TODO  业务数据接入逻辑
            String tname = Thread.currentThread().getName() + " --> partition-"+response.getUsedPartition();

            //  demo 为各分区接收数据量的统计， 实际业务开发在该处处理接收到的数据即可(*** 为保证数据不丢失：存储数据
            // 的同时，记录各分区的offset，offset可从response获取。服务、或者客户端发送异常时从记录偏移位置开始消费)
            if(countMap.get(tname) == null){
                countMap.put(tname,1L);
            }else{
                countMap.put(tname,countMap.get(tname) +1);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        System.out.println("服务端断开操作 ************************************************");
    }
    private JSONObject getJSONObject(String str){
        // 去掉多余信息
        JSONObject json = JSONObject.parseObject(str);
//        json.remove("content");
        return json;
    }
}
