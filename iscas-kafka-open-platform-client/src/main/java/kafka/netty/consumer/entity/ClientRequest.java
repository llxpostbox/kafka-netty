package kafka.netty.consumer.entity;

import java.util.concurrent.atomic.AtomicLong;

public class ClientRequest {
    //请求命令
    private String command="test";
    //请求参数
    private Object content;
    private final long id;
    //使用原子技术
    private static final AtomicLong al = new AtomicLong(0);

    public ClientRequest(){
        //请求唯一标识id 每次都会自增加1
        id = al.incrementAndGet();
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public long getId() {
        return id;
    }
}
