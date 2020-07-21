package iscas.kafka.data.open.platform.netty.db.dbutil;

import iscas.kafka.data.open.platform.netty.conf.NettyServiceConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


/**
 * @author  遇见清晨
 * @date 创建时间：2018年10月16日 上午9:17:52
 * @version 1.0
 */
public class DBUtil {

    private  static Properties conf = NettyServiceConfig.nettyConfig;

    /**
     * 获取数据库连接
     * @return 返回连接
     */
    public static Connection getConnection() {
    	// 数据库连接
    	Connection conn = null;
    	try {
			Class.forName(conf.getProperty("db.driver"));
			conn = DriverManager.getConnection(conf.getProperty("db.url"),
					conf.getProperty("db.username"), conf.getProperty("db.password"));
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return conn;
    }
    /**
     * 管不数据库连接
     * @param conn 数据库连接
     */
    public static void close(Connection conn) {
    	if(conn != null) {
    		try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }
}
