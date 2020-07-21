package iscas.kafka.data.open.platform.netty.content;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 运行通道上下文
 */
public class RunChannelContent {
    // 运行的通道
    public static Map<String,Boolean> channelMap = new ConcurrentHashMap<>();
}
