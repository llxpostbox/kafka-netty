package iscas.kafka.data.open.platform.netty.db.impl;

import iscas.kafka.data.open.platform.netty.bean.DbResponse;
import iscas.kafka.data.open.platform.netty.bean.User;
import iscas.kafka.data.open.platform.netty.db.UserDB;
import iscas.kafka.data.open.platform.netty.db.dbutil.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class UserImpl implements UserDB {

    @Override
    public DbResponse userCheckout(User user) {
        // 返回对象
        DbResponse dbResponse = new DbResponse();
        // 数据库连接
        Connection conn = null;
        PreparedStatement ps;
        // 查询
        try {
            // 创建连接
            conn = DBUtil.getConnection();
            // sql
            String sql = "SELECT * FROM t_user_topic_relevance "
                    + "WHERE userId = (SELECT id FROM t_kafka_cuser WHERE name=? AND `password`=? AND delstatus=1 AND isactive=1) "
                    + "AND topicId = (SELECT id FROM t_kafka_topic WHERE topic=? AND delstatus=1)";
            ps = conn.prepareStatement(sql);
            // 参数组装
            ps.setString(1,user.getName());
            ps.setString(2,user.getPassword());
            ps.setString(3,user.getTopic());
            // 查询数据
            ResultSet rs=ps.executeQuery();
            // 查询结果解析
            if(rs.next()){
                // 不为空
                dbResponse.setCheckoutResult(true);
                dbResponse.setMsg("用户及权限验证成功！");
            }else{
                dbResponse.setCheckoutResult(false);
                dbResponse.setMsg("用户及权限验证失败,未查询到用户名密码与TOPIC的绑定信息");
            }

            // 关闭数据库操作
            ps.close();
            rs.close();
        } catch (Exception e){
            e.printStackTrace();
            dbResponse.setCheckoutResult(false);
            dbResponse.setMsg(e.getMessage());
        } finally {
            DBUtil.close(conn);
        }
        return dbResponse;
    }
}
