package iscas.kafka.data.open.platform.netty.bean;

import java.io.Serializable;

public class User implements Serializable {

    // 用户第一请求参数
    private String name;
    private String password;
    private String topic;
    // 第一次请求返回
    private Integer totalPartition;
    private String token;
    // 第二次请求参数
    private Integer usedPartition;
    private Long offset;

    public User(){}
    public User(String name, String password, String topic){
        this.name = name;
        this.password = password;
        this.topic = topic;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getTotalPartition() {
        return totalPartition;
    }

    public void setTotalPartition(Integer totalPartition) {
        this.totalPartition = totalPartition;
    }

    public Integer getUsedPartition() {
        return usedPartition;
    }

    public void setUsedPartition(Integer usedPartition) {
        this.usedPartition = usedPartition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", password='" + password + '\'' +
                ", topic='" + topic + '\'' +
                ", totalPartition=" + totalPartition +
                ", token='" + token + '\'' +
                ", usedPartition=" + usedPartition +
                ", offset=" + offset +
                '}';
    }
}
