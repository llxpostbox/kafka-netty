package iscas.kafka.data.open.platform.netty.bean;

public class ServerRequest {

    private String command;

    private Object content;

    private  long id;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Object getContent() {

        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    @Override
    public String toString() {
        System.out.println("command:"+command+","+"id:"+id+","+"content:"+content);
        return super.toString();
    }
}
