package iscas.kafka.data.open.platform.netty.server;

import com.alibaba.fastjson.JSONObject;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import iscas.kafka.data.open.platform.netty.bean.DbResponse;
import iscas.kafka.data.open.platform.netty.content.ActiveUserContent;
import iscas.kafka.data.open.platform.netty.content.KafkaContent;
import iscas.kafka.data.open.platform.netty.content.LockContent;
import iscas.kafka.data.open.platform.netty.content.RunChannelContent;
import iscas.kafka.data.open.platform.netty.bean.Response;
import iscas.kafka.data.open.platform.netty.bean.ServerRequest;
import iscas.kafka.data.open.platform.netty.bean.User;
import iscas.kafka.data.open.platform.netty.core.KafkaConsumerDataSendThread;
import iscas.kafka.data.open.platform.netty.db.UserDB;
import iscas.kafka.data.open.platform.netty.db.impl.UserImpl;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class KafkaNettyServerHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = Logger.getLogger(KafkaNettyServerHandler.class);

    /**
     * 私有化一个用户请求变更上下文
     */
    private ActiveUserContent activeUserContent;

    public KafkaNettyServerHandler() {
        this.activeUserContent = new ActiveUserContent();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // 请求验证 处理
        if(msg instanceof ByteBuf){
            ByteBuf req = (ByteBuf)msg;
            String content = req.toString(Charset.defaultCharset());
            //获取客户端的请求信息  request 中拥有本次请求的 请求id、请求命令、请求内容（User）、
            ServerRequest request = JSONObject.parseObject(content,ServerRequest.class);
            User user = null;
            if(request.getContent() != null){
                user = JSONObject.parseObject(request.getContent().toString(),User.class);
            }
            // 用户信息检测
            if(user == null){
                serverResponse(ctx,request,500,"缺少用户信息");
                // 验证成功，关闭通道
                ctx.channel().close();
                return;
            }
            // 获取指令
            String command = request.getCommand();
            // 通道id
            String channelId = ctx.channel().id().toString();
            // kafka 上下文初始化， 从kafka获取最新topic分区情况
            KafkaContent.initKafkaContent();
            // 分区数查询
            if ("HANDSHAKE".equalsIgnoreCase(command)){ //
                user.setTotalPartition(KafkaContent.partitionMap.get(user.getTopic()));
                serverResponse(ctx,request,user,200,"分区获取成功");
                // 握手成功，关闭通道
                ctx.channel().close();
                return;
            }
            //
            LockContent.LEADER_VOTE_LOCK.lock();
            logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%  "+user.getName()+" --> "+user.getUsedPartition()+"获得锁  %%%%%%%%%%%%%%%%%%%%%%%%%%%");
            try{
                // 配置数缓存
                Map<String, User> channelUserMap = new HashMap<>();
                Map<String, Map<String,Integer>> topicChannelNumberMap = new HashMap<>();
                // 连接检测
                if("CHECKOUT".equalsIgnoreCase(command)){  // 验证请求
                    // 下载用户配置 -------  数据校验时下载配置
                    try {
                        activeUserContent.downloadConsumerConfig(channelUserMap,topicChannelNumberMap);
                    }catch (Exception e){
                        e.printStackTrace();
                        LockContent.LEADER_VOTE_LOCK.unlock();
                        logger.info("配置下载错误 --> "+user.getName()+" --> "+user.getUsedPartition()+"释放锁  .......");
                        return;
                    }
                    // 发送数据
                    if(topicChannelNumberMap.get(user.getName()) == null
                            || topicChannelNumberMap.get(user.getName()).get(user.getTopic()) == null
                            || KafkaContent.partitionMap.get(user.getTopic()) > topicChannelNumberMap.get(user.getName()).get(user.getTopic())){
                        // 用户信息校验
                        UserDB userDB = new UserImpl();
                        DbResponse dbResponse = userDB.userCheckout(user);
                        if(dbResponse.getCheckoutResult()){
                            // 活跃通道
                            RunChannelContent.channelMap.put(channelId,true);
                            user.setToken(channelId);
                            user.setTotalPartition(KafkaContent.partitionMap.get(user.getTopic()));
                            // 响应客户端
                            serverResponse(ctx,request,user,200,dbResponse.getMsg());
                        } else{
                            // 用户验证失败
                            serverResponse(ctx,request,500,dbResponse.getMsg());
                            ctx.channel().close();
                        }
                    } else {
                        // 分区已经使用完毕,关闭通道
                        serverResponse(ctx,request,202,"请求成功,但是分区已经使用完毕,不能消费数据!");
                        ctx.channel().close();
                    }

                }else if("RECEIVE".equalsIgnoreCase(command)){ // 接收请求
                    // token 验证
                    if(user.getToken() != null && RunChannelContent.channelMap.get(user.getToken()) != null
                            && RunChannelContent.channelMap.get(user.getToken())){ // 发过验证请求的
                        try {
                            activeUserContent.addChannelBound(channelId,user);
                        } catch (Exception e){
                            e.printStackTrace();
                            LockContent.LEADER_VOTE_LOCK.unlock();
                            logger.info("新增用户信息错误 --> "+user.getName()+" --> "+user.getUsedPartition()+"释放锁  .......");
                            return;
                        }
                        // 消费线程创建成功后更新 consumer 节点信息
                        new KafkaConsumerDataSendThread(ctx,request,channelId,user).start();
                        serverResponse(ctx,request,200,"连接成功!!!");
                    } else {
                        // 分区已经使用完毕,关闭通道
                        serverResponse(ctx,request,500,"没有 token,请先发送用户验证请求!");
                        ctx.channel().close();
                    }

                } else {
                    serverResponse(ctx,request,500,"错误指令："+command);
                    ctx.channel().close();
                }
            }catch (Exception e){e.printStackTrace();}finally {
                LockContent.LEADER_VOTE_LOCK.unlock();
                logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%  "+user.getName()+" --> "+user.getUsedPartition()+"释放得锁  %%%%%%%%%%%%%%%%%%%%%%%%%%%");
            }
        }else{
            logger.info("%%%%%%%%%%%%%%%%%%%%%%%%%%%% 缺少用户信息, telnet or ping %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        }
    }

    /**
     * 验证响应
     */
    private static void serverResponse(ChannelHandlerContext ctx, ServerRequest request, User user, int code, String msg){
        Response response = new Response();
        // 请求id
        response.setId(request.getId());
        response.setContent(user);
        response.setStatus(code);
        response.setMsg(msg);
        //先写入
        ctx.channel().writeAndFlush(JSONObject.toJSONString(response) + "\r\n");
    }
    /**
     * 验证响应
     */
    public static void serverResponse(ChannelHandlerContext ctx, ServerRequest request,int code, String msg){

        Response response = new Response();
        // 请求id
        response.setId(request.getId());
        response.setContent(request.getContent());
        response.setStatus(code);
        response.setMsg(msg);
        //先写入
        StringBuilder line = new StringBuilder();
        line.append(JSONObject.toJSONString(response)).append("\r\n");
        System.out.println(line.toString());
        ctx.channel().writeAndFlush(line.toString());
    }
    /**
     * channelInactive
     *
     * channel 通道 Inactive 不活跃的
     *
     * 当客户端主动断开服务端的链接后，这个通道就是不活跃的。也就是说客户端与服务端的关闭了通信通道并且不可以传输数据
     *
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx){
        System.out.println(ctx.channel().localAddress().toString() + " 通道不活跃！");
        String channelId = ctx.channel().id().toString();
        // 移除通道
        RunChannelContent.channelMap.remove(channelId);  // || RunChannelContent.channelMap.pur(channelId, false);
        // 移除通道-用户绑定
        activeUserContent.removeChannelBound(channelId);
        // 关闭流+
        ctx.close();
    }
    /**
     * exceptionCaught exception 异常 Caught 抓住
     * 抓住异常，当发生异常的时候，可以做一些相应的处理，比如打印日志、关闭链接
     */
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        //super.exceptionCaught(ctx, cause);
        ctx.close();
        System.out.println("异常信息：\r\n" + cause.getMessage());

    }
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
            throws Exception {

//        if(evt instanceof IdleStateEvent){
//            IdleStateEvent event = (IdleStateEvent)evt;
//            if(event.equals(IdleState.READER_IDLE)){
//                System.out.println("读空闲====");
//                ctx.close();
//            }else if(event.equals(IdleState.WRITER_IDLE)){
//                System.out.println("写空闲====");
//            }else if(event.equals(IdleState.WRITER_IDLE)){
//                System.out.println("读写空闲====");
//                ctx.channel().writeAndFlush("ping\r\n");
//            }
//
//        }

        super.userEventTriggered(ctx, evt);
    }
}
