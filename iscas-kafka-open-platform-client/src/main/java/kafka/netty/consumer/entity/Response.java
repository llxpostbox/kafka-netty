package kafka.netty.consumer.entity;

public class Response {
    private Long id;//请求ID
    private int status;//响应状态
    private Object content;//响应内容
    private String msg;//请求返回信息
    private Integer usedPartition;
    private long offset;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Object getContent() {
        return content;
    }

    public void setContent(Object content) {
        this.content = content;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Integer getUsedPartition() {
        return usedPartition;
    }

    public void setUsedPartition(Integer usedPartition) {
        this.usedPartition = usedPartition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Response{" +
                "id=" + id +
                ", status=" + status +
                ", content=" + content +
                ", msg='" + msg + '\'' +
                ", usedPartition=" + usedPartition +
                ", offset=" + offset +
                '}';
    }
}
