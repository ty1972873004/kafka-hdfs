package com.hncy58.ds;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

public class DataSourceUtil {

	// 持有一个静态的数据库连接池对象
	private static DataSource DS = null;
	private static boolean inited = false;

	// 使用DBCP提供的BasicDataSource实现DataSource接口
	public static void initDataSource(String connectURI, String username, String password, String driverClass,
			int initialSize, int maxTotal, int maxIdle, int maxWaitMillis) {
		synchronized (DataSourceUtil.class) {
			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(driverClass);
			ds.setUsername(username);
			ds.setPassword(password);
			ds.setUrl(connectURI);
			ds.setInitialSize(initialSize);
			ds.setMaxTotal(maxTotal);
			ds.setMaxIdle(maxIdle);
			ds.setMaxWaitMillis(maxWaitMillis);
			DS = ds;
			inited = true;
		}
	}

	public static Connection getConnection() throws SQLException {

		if(! inited) {
			throw new SQLException("连接池未初始化！");
		}
		
		Connection con = null;
		if (DS != null) {
			try {
				con = DS.getConnection();
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
			// 将数据库连接的事物设置为不默认为自动Commit
			try {
				con.setAutoCommit(false);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			return con;
		}
		// 回收数据库连接时，直接使用con.close()即可
		return con;

	}

	public static void shutdownDataSource() throws SQLException {
		BasicDataSource bds = (BasicDataSource) DS;
		bds.close();
	}
}
