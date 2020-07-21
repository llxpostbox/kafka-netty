package iscas.kafka.data.open.platform.netty.content;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class ConsumerDataCount {
    private static final Logger logger = Logger.getLogger(ConsumerDataCount.class);

    public static ConcurrentHashMap<String,Double> countMap = new ConcurrentHashMap<>();
    private static Timer timer = new Timer();
    static {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                double total = 0;
                for (Map.Entry<String,Double> entry : countMap.entrySet()){
                    logger.info("数据消费: "+entry.getKey()+" = "+entry.getValue());
                    total += entry.getValue();
                }
                logger.info("总消费数据: "+total);
            }
        },1000,5000);
    }
}
