package iscas.kafka.data.open.platform.netty.zk;

import com.google.common.base.Charsets;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

public class MyZkSerializer implements ZkSerializer {
    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        return String.valueOf(data).getBytes(Charsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes, Charsets.UTF_8);
    }
}
