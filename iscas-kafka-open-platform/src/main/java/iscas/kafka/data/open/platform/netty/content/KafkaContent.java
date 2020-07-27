package iscas.kafka.data.open.platform.netty.content;

import iscas.kafka.data.open.platform.netty.conf.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaContent {
    // 运行的通道数控制, 没个用户创建的运行通道数为上传的topic的最大分区数，运行通道达到最大分区时，不在创建通道
    // 服务启动时加载kafka上下文
    public static Map<String, Integer> partitionMap = new ConcurrentHashMap<>();
    static{
        Logger.getLogger("org.apache.kafka").setLevel(Level.OFF);
    }
    public static void initKafkaContent(){
        //  kafka上线问初始化业务
        KafkaConsumer<String, String> kafkaConsumer = null;
        try{
            // kafka consumer builder
            kafkaConsumer = new KafkaConsumer<>(KafkaConfig.getKafkaMiniConf());
            Map<String, List<PartitionInfo>> kafkaTopicsMap = kafkaConsumer.listTopics();
            for (Map.Entry<String, List<PartitionInfo>> entry : kafkaTopicsMap.entrySet()){
                partitionMap.put(entry.getKey(),entry.getValue().size());
            }
            // kafka上下文初始化成功
            System.out.println("kafka content init success!");
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("kafka 上下文加载失败，退出服务");
        } finally {
            if(kafkaConsumer != null){
                kafkaConsumer.close();
            }
        }
    }
}
