package iscas.kafka.data.open.platform.netty.conf;

import java.util.Properties;

public class KafkaConfig {

    private static Properties kafkaDetaulConf = NettyServiceConfig.nettyConfig;
    private static Properties kafkaMiniConf = new Properties();

    public static void initKafkaConf(){
        // kafka consumer mini config
        kafkaMiniConf.put("bootstrap.servers",kafkaDetaulConf.getProperty("bootstrap.servers"));
        kafkaMiniConf.put("key.deserializer",kafkaDetaulConf.getProperty("key.deserializer"));
        kafkaMiniConf.put("value.deserializer",kafkaDetaulConf.getProperty("value.deserializer"));
        // 配置初始化成功
        System.out.println("kafka config init success!");
    }

    public static Properties getKafkaDetaulConf() {
        return kafkaDetaulConf;
    }

    public static void setKafkaDetaulConf(Properties kafkaDetaulConf) {
        KafkaConfig.kafkaDetaulConf = kafkaDetaulConf;
    }

    public static Properties getKafkaMiniConf() {
        return kafkaMiniConf;
    }

    public static void setKafkaMiniConf(Properties kafkaMiniConf) {
        KafkaConfig.kafkaMiniConf = kafkaMiniConf;
    }
}
