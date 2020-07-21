package kafka.netty.consumer.conf;

import java.io.IOException;
import java.util.Properties;

public class ClientConfig {
    public static Properties conf = new Properties();
    static {
        try {
            conf.load(ClientConfig.class.getResourceAsStream("/data-receive.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
