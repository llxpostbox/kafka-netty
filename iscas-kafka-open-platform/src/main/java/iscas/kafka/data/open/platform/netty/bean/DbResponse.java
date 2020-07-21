package iscas.kafka.data.open.platform.netty.bean;

public class DbResponse {

    private Boolean checkoutResult = false;

    private String msg;

    public Boolean getCheckoutResult() {
        return checkoutResult;
    }

    public void setCheckoutResult(Boolean checkoutResult) {
        this.checkoutResult = checkoutResult;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "DbResponse{" +
                "checkoutResult=" + checkoutResult +
                ", msg=" + msg +
                '}';
    }
}
