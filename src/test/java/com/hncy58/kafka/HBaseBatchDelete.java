package com.hncy58.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class HBaseBatchDelete {

	private static String zkServers = "192.168.144.128";
	private static String zkPort = "2181";

	public static void main(String[] args) {
		deleteTimeRange("ccs:customer", (System.currentTimeMillis() - 19600 * 1000), System.currentTimeMillis());
	}

	/**
	 * 删除一段时间的表记录
	 *
	 * @param c
	 * @param minTime
	 * @param maxTime
	 */
	public static void deleteTimeRange(String tableName, Long minTime, Long maxTime) {
		Table table = null;
		Connection connection = null;
		Configuration hbaseConf = HBaseConfiguration.create();
		hbaseConf.set("hbase.zookeeper.quorum", zkServers);
		hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
		hbaseConf.set("hbase.defaults.for.version.skip", "true");
		try {
			connection = ConnectionFactory.createConnection(hbaseConf);
			Scan scan = new Scan();
			scan.setTimeRange(minTime, maxTime);
			table = connection.getTable(TableName.valueOf(tableName));

			long start = System.currentTimeMillis();
			ResultScanner rs = table.getScanner(scan);
			List<Delete> list = getDeleteList(rs);
			System.out.println("total used " + (System.currentTimeMillis() - start) + " ms.");
			System.out.println("scan size:" + list.size());
			if (list.size() > 0) {
				start = System.currentTimeMillis();
				table.delete(list.subList(0, list.size() > 50000 ? 50000 : list.size()));
				System.out.println("total used " + (System.currentTimeMillis() - start) + " ms.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != table) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static List<Delete> getDeleteList(ResultScanner rs) {

		List<Delete> list = new ArrayList<>();
		try {
			for (Result r : rs) {
				Delete d = new Delete(r.getRow());
				list.add(d);
			}
		} finally {
			rs.close();
		}
		return list;
	}

}
