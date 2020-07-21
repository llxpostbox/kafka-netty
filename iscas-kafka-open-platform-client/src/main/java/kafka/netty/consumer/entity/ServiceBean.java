package kafka.netty.consumer.entity;

import java.io.Serializable;

public class ServiceBean implements Serializable {

    private String host;
    private int post;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPost() {
        return post;
    }

    public void setPost(int post) {
        this.post = post;
    }

}
