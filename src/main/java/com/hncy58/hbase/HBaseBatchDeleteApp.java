package com.hncy58.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
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

public class HBaseBatchDeleteApp {

	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat dateSdf = new SimpleDateFormat("yyyy-MM-dd");

	private static String zkServers = "192.168.144.128";
	private static String zkPort = "2181";

	private static String db = "ccs";
	private static String table = "order";
	private static String startDate = "";
	private static String startTime = "";
	private static String endDate = "";
	private static String endTime = "";

	private static int maxDeleteBatch = 20000;

	public static void main(String[] args) throws Exception {

		System.out.println("Usage:\n" + HBaseBatchDeleteApp.class.getName()
				+ " zkServers zkPort db table startDate startTime endDate endTime maxDeleteBatch");
		System.out.println("eg:\n" + HBaseBatchDeleteApp.class.getName() + " " + zkServers + " " + zkPort + " " + db
				+ " " + table + " " + dateSdf.format(new Date()) + " 00:00:00 " + dateSdf.format(new Date())
				+ " 23:59:59 " + maxDeleteBatch);

		if (args.length > 0) {
			zkServers = args[0].trim();
		}

		if (args.length > 1) {
			zkPort = args[1].trim();
		}

		if (args.length > 2) {
			db = args[2].trim();
		}

		if (args.length > 3) {
			table = args[3].trim();
		}

		if (args.length > 4) {
			startDate = args[4].trim();
		}

		if (args.length > 5) {
			startTime = args[5].trim();
		}

		if (args.length > 6) {
			endDate = args[6].trim();
		}

		if (args.length > 7) {
			endTime = args[7].trim();
		}

		if (args.length > 8) {
			maxDeleteBatch = Integer.parseInt(args[8].trim());
		}

		String realTable = (isEmpty(db)) ? table : db + ":" + table;
		long start = 0;
		long end = 0;

		if (isEmpty(realTable)) {
			System.out.println("需要删除的表没有配置正确，请检查！");
			return;
		}

		if (!isEmpty(startDate) && !isEmpty(startTime)) {
			start = sdf.parse(startDate + " " + startTime).getTime();
		} else if (!isEmpty(startDate)) {
			start = sdf.parse(startDate + " 00:00:00").getTime();
		} else {
			start = sdf.parse(dateSdf.format(new Date()) + " 00:00:00").getTime();
		}

		if (!isEmpty(endDate) && !isEmpty(endTime)) {
			end = sdf.parse(endDate + " " + endTime).getTime();
		} else if (!isEmpty(endDate)) {
			end = sdf.parse(endDate + " 23:59:59").getTime();
		} else {
			end = sdf.parse(dateSdf.format(new Date()) + " 23:59:59").getTime();
		}

		deleteTimeRange(realTable, start, end);
	}

	private static boolean isEmpty(String str) {
		return str == null || "".equals(str.trim());
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
			scan.setBatch(10000);
//			scan.setRaw(false);
//			scan.setCaching(10000);
			table = connection.getTable(TableName.valueOf(tableName));

			long start = System.currentTimeMillis();
			ResultScanner rs = table.getScanner(scan);
			List<Delete> list = getDeleteList(rs);
			System.out.println(
					"scan table " + tableName + " total used " + (System.currentTimeMillis() - start) + " ms.");
			System.out.println("scan table " + tableName + " size -> " + list.size());

			if (list.isEmpty()) {
				System.out.println("table " + tableName + " has no data to delete. ignore it.");
				return;
			}

			Iterator<Delete> delIt = list.iterator();
			List<Delete> batchDeletes = new ArrayList<>();

			while (delIt.hasNext()) {
				batchDeletes.add(delIt.next());
				if (batchDeletes.size() >= maxDeleteBatch) {
					start = System.currentTimeMillis();
					table.delete(batchDeletes);
					System.out.println("batch delete used " + (System.currentTimeMillis() - start)
							+ " ms. batch size -> " + batchDeletes.size());
					batchDeletes.clear();
				}
			}

			if (!batchDeletes.isEmpty()) {
				start = System.currentTimeMillis();
				table.delete(batchDeletes);
				System.out.println("batch delete used " + (System.currentTimeMillis() - start) + " ms. batch size -> "
						+ batchDeletes.size());
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
