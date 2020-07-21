package iscas.kafka.data.open.platform.netty.core;

import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandlerContext;
import iscas.kafka.data.open.platform.netty.bean.ServerRequest;
import iscas.kafka.data.open.platform.netty.conf.KafkaConfig;
import iscas.kafka.data.open.platform.netty.content.ConsumerDataCount;
import iscas.kafka.data.open.platform.netty.content.RunChannelContent;
import iscas.kafka.data.open.platform.netty.bean.Response;
import iscas.kafka.data.open.platform.netty.bean.User;
import iscas.kafka.data.open.platform.netty.server.KafkaNettyServerHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerDataSendThread extends Thread{
    private static final Logger logger = Logger.getLogger(KafkaConsumerDataSendThread.class);

    private String TOPIC;
    private static Properties kafkaProps;
    private ChannelHandlerContext ctx;
    private ServerRequest request;
    private String channelId;
    private User user;

    public KafkaConsumerDataSendThread(ChannelHandlerContext ctx, ServerRequest request,String channelId, User user){
        this.user = user;
        this.request = request;
        this.ctx = ctx;
        this.channelId = channelId;
        this.TOPIC = user.getTopic();
        kafkaInit();
    }

    private void kafkaInit() {
        kafkaProps = (Properties) KafkaConfig.getKafkaDetaulConf().clone();
//        kafkaProps.put("bootstrap.servers", "172.168.16.244:9092,172.168.16.245:9092,172.168.16.251:9092");
        // 消费组ID
        kafkaProps.put("group.id", user.getName());   // 生产环境使用该方法， 即使用用户名称为消费者（消费组）名称
        // 如果为真，消费者的偏移量将在后台定期提交。
//        kafkaProps.put("enable.auto.commit", "true"); // 默认为 true
        // 如果enable.auto.commit被设置为true，那么消费者偏移量自动提交到Kafka的毫秒频率。
//        kafkaProps.put("auto.commit.interval.ms", "1000"); // 默认 5000 = 5秒
        // 使用Kafka的组管理功能时用于检测使用者故障的超时。使用者向代理发送周期性的心跳，以指示其活动状态。
        // 如果在此会话超时过期之前代理没有收到心跳，则代理将从组中删除此使用者并启动重新平衡。请注意，该值
        // 必须在代理配置中配置的允许范围内。group.min.session.timeout.ms和group.max.session.timeout.ms。
//        kafkaProps.put("session.timeout.ms", "10000"); // 默认10000
        // 明了使用何种序列化方式将用户提供的key和vaule值序列化成字节。
//        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 明了使用何种序列化方式将用户提供的key和vaule值序列化成字节。
//        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    }

    @Override
    public void run() {
        // 分区检测
        if (user.getUsedPartition() != null && user.getUsedPartition() >= user.getTotalPartition()
                || user.getUsedPartition() != null && user.getUsedPartition() < 0 ){ // 使用的分区信息错误
            // 分区检测
            KafkaNettyServerHandler.serverResponse(ctx,request,500,"使用分区错误, 未发现 > "+user.getUsedPartition()+" 的分区信息");
            // 停止线程
            return;
        }
        // 未填写使用分区时
        kafkaDataSend(user.getUsedPartition());
    }

    /**
     * 所有分区
     */
    private void kafkaDataSend(Integer usedPartition) {
        // kafka 消费者创建
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        if(usedPartition == null){
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC));
        }else{
            TopicPartition partition = new TopicPartition(TOPIC,usedPartition);
            kafkaConsumer.assign(Collections.singletonList(partition));
            if(user.getOffset() != null){ // 设置了偏移位置则从偏移位置开始读取
                kafkaConsumer.seek(partition,user.getOffset());
            }
        }
//        System.out.println("Subscribed to topic:" + TOPIC);
//        logger.info(channelId+" >>> kafka 连接成功, 正在消费数据: "+ kafkaConsumer.listTopics());

        double index = 0;
        while (true) {
            // 通道是否正常检查
            if(RunChannelContent.channelMap.get(channelId) == null || !RunChannelContent.channelMap.get(channelId)){
                // 通道已关闭，不在发送数据
                logger.info(Thread.currentThread().getName()+" [ "+channelId+" ] 通道关闭，不在发送数据!!");
                logger.info(Thread.currentThread().getName() +" server send to "+channelId+"： >>>>>> 总数据量： "+index);
                break;
            }
            // 消费数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));  // 100 消费者拉取数据的频率
            StringBuilder line = new StringBuilder();
            for (ConsumerRecord<String, String> record : records) {
                // print the offset, key and value for the consumer records
//                System.out.printf("Offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                index ++;
                Response res = new Response();
                res.setId(-1L);
                res.setStatus(200);
                res.setMsg("consumer running");
                res.setContent(record.value());
                res.setUsedPartition(record.partition());  // 传入分区时  usedPartition == record.value()
                res.setOffset(record.offset());
                line.append(JSONObject.toJSONString(res)).append("\r\n");

                if(index % 100 == 0){
                    // 每1000条数据写出一次
                    ctx.channel().writeAndFlush(line.toString());
                    // 发送数据量
//                    logger.info(Thread.currentThread().getName() +" server send to "+channelId+"： >>>>>> 总数据量： "+index);
                    line = new StringBuilder();
                }
            }
            // 未写入完毕的数据检查
            if(line.length() > 0){
                // 每1000条数据写出一次
                ctx.channel().writeAndFlush(line.toString());
                // 发送数据量
//                logger.info(Thread.currentThread().getName() +" server send to "+channelId+"： >>>>>> 总数据量： "+index);
            }
            //
            ConsumerDataCount.countMap.put(user.getName()+"-->"+user.getTopic()+"->"+user.getUsedPartition(),index);
        }
        // 关闭kafka连接
        kafkaConsumer.close();
        // 关闭当前线程
        Thread.currentThread().interrupt();
    }
}
