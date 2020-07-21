package iscas.kafka.data.open.platform.netty.db;

import iscas.kafka.data.open.platform.netty.bean.DbResponse;
import iscas.kafka.data.open.platform.netty.bean.User;

public interface UserDB {

    DbResponse userCheckout(User user);
}
