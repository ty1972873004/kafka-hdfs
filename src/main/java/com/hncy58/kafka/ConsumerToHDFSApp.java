package com.hncy58.kafka;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.ServerStatusReportUtil;
import com.hncy58.heartbeat.HeartRunnable;

public class ConsumerToHDFSApp {

	private static int FETCH_MILISECONDS = 1000;
	private static int SLEEP_SECONDS = 5;
	private static int MIN_BATCH_SIZE = 5000;
	private static int MIN_SLEEP_CNT = 5;
	private static int OFFSET_COMMIT_RETRY_CNT = 3;
	private static int OFFSET_COMMIT_RETRY_INTERVAL = 3;
	private static Long TOTAL_MSG_CNT = 0L;
	private static Long ERR_HANDLED_CNT = 0L;

	private static String localFileNamePrefix = "unHadledData";

	private static String agentSvrName = "testname";
	private static String agentSvrGroup = "testgrp";
	private static int agentSvrType = 2;
	private static int agentSourceType = 2;
	private static int agentDestType = 2;

	private static String kafkaServers = "localhost:9092";
	private static String kafkaGroupId = ConsumerToHDFSApp.class.getSimpleName();
	private static List<String> subscribeToipcs = new ArrayList<>();

	private static boolean run = false;
	private static Configuration hadoopConf = new Configuration(true);
	private static KafkaConsumer<String, String> consumer;

	private static final Logger log = LoggerFactory.getLogger(ConsumerToHDFSApp.class);
	private static String HDFS_PREFIX_PATH = "hdfs://node01:8020/tmp/";

	private static boolean shutdown_singal = false;
	public static boolean shutdown = false;
	private static Thread heartThread;
	private static HeartRunnable heartRunnable;

	public static void setDown(boolean flag) {
		shutdown_singal = flag;
	}

	public static boolean getDown() {
		return shutdown_singal;
	}

	public static void main(String[] args) {

		ConsumerToHDFSApp app = new ConsumerToHDFSApp();

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				while (!ConsumerToHDFSApp.shutdown) {
					try {
						log.error("监测到中断进程信号，设置服务为下线！");
						ConsumerToHDFSApp.setDown(true);
						Thread.sleep(2 * 1000);
					} catch (InterruptedException e) {
						log.error(e.getMessage(), e);
					}
				}
			}
		}, "ShutdownHookThread"));

		// 开始运行
		app.doRun(args);

	}

	public void doRun(String[] args) {

		init(args);

		try {
			List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
			int sleepdCnt = 0;

			while (run) {
				try {
					// 如果发送了终止进程消息，则停止消费，并且处理掉缓存的消息
					if (shutdown_singal || ERR_HANDLED_CNT >= 5) {
						run = false;
						try {
							if (buffer != null && !buffer.isEmpty()) {
								doHandle(buffer);
								buffer.clear();
							}
							// 停止状态上报线程
							heartRunnable.setRun(false);
							heartRunnable.setSvrStatus(0);
							heartThread.interrupt();

							boolean ret = ServerStatusReportUtil.reportSvrStatus(agentSvrName, agentSvrGroup,
									agentSvrType, 0, "监测到服务中断信号，退出服务！");
							log.info("设置服务状态为下线：" + ret);
							ret = ServerStatusReportUtil.reportAlarm(agentSvrName, agentSvrGroup, agentSvrType, 1, 4,
									"设置服务状态为下线：" + ret + "，shutdown_singal：" + shutdown_singal + "，ERR_HANDLED_CNT："
											+ ERR_HANDLED_CNT);
							log.info("上报告警结果：" + ret);

							shutdown = true;
							log.error("监测到服务中断信号，退出服务！");
							System.exit(0);
						} catch (Exception e) {
							log.error(e.getMessage(), e);
							log.error("捕获到异常停止状态，直接退出进程！");
							System.exit(1);
						}
					} else {
						ConsumerRecords<String, String> records = consumer.poll(FETCH_MILISECONDS);
						int cnt = records.count();
						if (cnt > 0) {
							log.error("current polled " + cnt + " records.");
							TOTAL_MSG_CNT += cnt;
							log.error("total polled " + TOTAL_MSG_CNT + " records.");
							for (ConsumerRecord<String, String> record : records) {
								buffer.add(record);
							}

							if (buffer.size() >= MIN_BATCH_SIZE || (sleepdCnt >= MIN_SLEEP_CNT && !buffer.isEmpty())) {
								sleepdCnt = 0;
								doHandle(buffer);
								buffer.clear();
								Thread.sleep(500); //
							} else {
								log.error("current buffer remains " + buffer.size() + " records.");
								sleepdCnt += 1;
							}
						} else {
							log.error("no data to poll, sleep " + SLEEP_SECONDS + " s. buff size:" + buffer.size());
							if ((sleepdCnt >= MIN_SLEEP_CNT && !buffer.isEmpty())) {
								sleepdCnt = 0;
								doHandle(buffer);
								buffer.clear();
							} else {
								Thread.sleep(SLEEP_SECONDS * 1000);
								sleepdCnt += 1;
							}
						}

						if (TOTAL_MSG_CNT >= Long.MAX_VALUE)
							TOTAL_MSG_CNT = 0L; // 达到最大值后重置为0
					}
				} catch (Exception e) {
					log.error("消费、处理数据异常:" + e.getMessage(), e);
				}
			}
		} catch (Exception e) {
			log.error("主流程捕获到异常：" + e.getMessage(), e);
		} finally {
			consumer.close();
		}
	}

	/**
	 * 初始化程序
	 * 
	 * @param args
	 */
	private static void init(String[] args) {

		log.info("usage:" + ConsumerToHDFSApp.class.getName()
				+ " kafkaServers kafkaTopicGroupName kafkaToipcs FETCH_MILISECONDS MIN_BATCH_SIZE MIN_SLEEP_CNT SLEEP_SECONDS");
		log.info("eg:" + ConsumerToHDFSApp.class.getName()
				+ " localhost:9092 kafka_hdfs_group_2 test-topic-1 1000 5000 3 5");

		int ret = ServerStatusReportUtil.register(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
				agentDestType);

		while (ret != 1) {
			log.error("注册服务失败，name:{}, group:{}, svrType:{}, sourceType:{}, destType:{}, 注册结果:{}", agentSvrName,
					agentSvrGroup, agentSvrType, agentSourceType, agentDestType, ret);
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
			ret = ServerStatusReportUtil.register(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType,
					agentDestType);
		}

		log.info("注册代理服务结果(-1:fail, 1:success, 2:standby) -> {}", ret);

		heartRunnable = new HeartRunnable(agentSvrName, agentSvrGroup, agentSvrType, agentSourceType, agentDestType);
		heartThread = new Thread(heartRunnable, "agentSvrStatusReportThread");
		heartThread.start();

		log.info("启动代理服务状态定时上报线程:" + heartThread.getName());

		hadoopConf.set("dfs.support.append", "true");
		hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

		// for test
		hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");

		Properties props = new Properties();

		if (args.length > 0) {
			kafkaServers = args[0].trim();
		} else {
			kafkaServers = "192.168.144.128:9092";
		}

		if (args.length > 1) {
			kafkaGroupId = args[1].trim();
		} else {
			kafkaGroupId = "kafka_hdfs_group_2";
		}

		if (args.length > 2) {
			subscribeToipcs = Arrays.asList(args[2].trim().split(" *, *"));
		} else {
			// for test
			subscribeToipcs.add("test-topic-1");
			subscribeToipcs.add("test-topic-2");
		}

		if (args.length > 3) {
			HDFS_PREFIX_PATH = args[3].trim();
		}

		if (args.length > 4) {
			FETCH_MILISECONDS = Integer.parseInt(args[4].trim());
		}

		if (args.length > 5) {
			MIN_BATCH_SIZE = Integer.parseInt(args[5].trim());
		}

		if (args.length > 6) {
			MIN_SLEEP_CNT = Integer.parseInt(args[6].trim());
		}

		if (args.length > 7) {
			SLEEP_SECONDS = Integer.parseInt(args[7].trim());
		}

		props.put("bootstrap.servers", kafkaServers);
		props.put("group.id", kafkaGroupId);

		// props.put("auto.commit.interval.ms", "1000");
		props.put("enable.auto.commit", "false");
		props.put("isolation.level", "read_committed");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(subscribeToipcs);
		run = true;
	}

	private static void doHandle(List<ConsumerRecord<String, String>> buffer) throws Exception {

		int offsetCommitRetryCnt = 0;
		boolean success = false;
		while (!success && offsetCommitRetryCnt < OFFSET_COMMIT_RETRY_CNT) {
			try {
				handle(buffer);
				success = true;
			} catch (Exception e) {
				log.error("处理数据异常，重试次数：" + offsetCommitRetryCnt + "，错误信息：" + e.getMessage(), e);
				Thread.sleep(OFFSET_COMMIT_RETRY_INTERVAL * 1000);
			} finally {
				offsetCommitRetryCnt += 1;
			}
		}

		if (!success)
			ERR_HANDLED_CNT += 1;

		boolean cmtSuccess = commitOffsets();
		// 如果重试N次还是失败且偏移量提交成功，则记录到本地文件，然后发送告警信息到监控服务
		// 如果重试N次提交偏移量还是失败，则记录到本地文件，然后发送告警信息到监控服务
		if (!success && cmtSuccess) {
			success = doSaveToLocalFile(buffer);
		}

		if (!success) {
			log.error("经过重试写入未提交消息到本地文件失败，最终放弃，数据如下：{}", buffer);
		}
	}

	private static void handle(List<ConsumerRecord<String, String>> buffer) throws Exception {

		Map<String, StringBuffer> buffMap = new HashMap<>();

		try {
			log.error("start to handle datas -> " + buffer.size());
			buffer.forEach(record -> {
				String tmpStr = (record.timestamp() + "," + record.partition() + "," + record.offset() + ","
						+ record.key() + "," + record.value() + "\n");
				if (buffMap.containsKey(record.topic())) {
					buffMap.get(record.topic()).append(tmpStr);
				} else {
					StringBuffer buf = new StringBuffer();
					buf.append(tmpStr);
					buffMap.put(record.topic(), buf);
				}
			});

			if (!buffMap.isEmpty()) {
				for (Entry<String, StringBuffer> entry : buffMap.entrySet()) {
					String topic = entry.getKey();
					if (entry.getValue().length() > 0) {
						String hdfs_path = HDFS_PREFIX_PATH + topic + "/"
								+ new SimpleDateFormat("yyyyMMddHH").format(new Date());
						Path filePath = new Path(hdfs_path);
						FileSystem fs = null;
						OutputStream out = null;
						try {
							fs = FileSystem.get(new URI(hdfs_path), hadoopConf);
							if (!fs.exists(filePath)) {
								out = fs.create(filePath, false);
							} else {
								out = fs.append(filePath);
							}
							IOUtils.copyBytes(new ByteArrayInputStream(entry.getValue().toString().getBytes("UTF-8")),
									out, 4096, true);
							// out.flush();
						} finally {
							// IOUtils.closeStream(out);
							// out = null;
							// IOUtils.closeStream(fs);
							// fs.close();
							// fs = null;
						}
					}
				}
			}

			log.error("end handled datas.");
		} finally {

		}
	}

	/**
	 * 可重试N次提交偏移量
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	private static boolean commitOffsets() throws InterruptedException {
		int offsetCommitRetryCnt = 1;
		boolean success = false;
		while (!success && offsetCommitRetryCnt < OFFSET_COMMIT_RETRY_CNT) {
			try {
				consumer.commitSync();
				success = true;
			} catch (Exception e) {
				log.error("消费成功数据，但提交偏移量失败，重试次数：" + offsetCommitRetryCnt + "，错误信息：" + e.getMessage(), e);
				Thread.sleep(OFFSET_COMMIT_RETRY_INTERVAL * 1000);
			} finally {
				offsetCommitRetryCnt += 1;
			}
		}
		return success;
	}

	/**
	 * 重试N次保存未成功提交偏移量的数据
	 * 
	 * @param buffer
	 * @return
	 * @throws InterruptedException
	 */
	private static boolean doSaveToLocalFile(List<ConsumerRecord<String, String>> buffer) throws InterruptedException {

		int offsetCommitRetryCnt = 1;
		boolean success = false;
		while (!success && offsetCommitRetryCnt < OFFSET_COMMIT_RETRY_CNT) {
			try {
				saveToLocalFile(buffer);
				success = true;
			} catch (Exception e) {
				log.error("写入未处理数据到本地文件失败，重试次数：" + offsetCommitRetryCnt + "，错误信息：" + e.getMessage(), e);
				Thread.sleep(OFFSET_COMMIT_RETRY_INTERVAL * 1000);
			} finally {
				offsetCommitRetryCnt += 1;
			}
		}
		return success;
	}

	/**
	 * 保存未成功提交偏移量的数据
	 * 
	 * @param buffer
	 * @return
	 * @throws Exception
	 */
	private static boolean saveToLocalFile(List<ConsumerRecord<String, String>> buffer) throws Exception {

		boolean success = false;
		Map<String, StringBuffer> buffMap = new HashMap<>();

		try {
			log.error("start to handle datas -> " + buffer.size());
			buffer.forEach(record -> {
				String tmpStr = (record.timestamp() + "," + record.partition() + "," + record.offset() + ","
						+ record.key() + "," + record.value() + "\n");
				if (buffMap.containsKey(record.topic())) {
					buffMap.get(record.topic()).append(tmpStr);
				} else {
					StringBuffer buf = new StringBuffer();
					buf.append(tmpStr);
					buffMap.put(record.topic(), buf);
				}
			});

			if (!buffMap.isEmpty()) {
				for (Entry<String, StringBuffer> entry : buffMap.entrySet()) {
					String topic = entry.getKey();
					if (entry.getValue().length() > 0) {
						BufferedOutputStream bos = null;
						if (buffer == null || buffer.isEmpty())
							return !success;

						try {
							long startOffset = buffer.get(0).offset();
							long endOffset = buffer.get(buffer.size() - 1).offset();
							String fileName = localFileNamePrefix + "_" + topic + "_"
									+ new SimpleDateFormat("yyyyMMddHH").format(new Date()) + "_" + startOffset + "-"
									+ endOffset;
							bos = new BufferedOutputStream(new FileOutputStream(fileName, true));

							StringBuffer buf = new StringBuffer();
							buffer.forEach(record -> {
								buf.append((record.timestamp() + "," + record.partition() + "," + record.offset() + ","
										+ record.key() + "," + record.value() + "\n"));
							});

							if (buf.length() > 0) {
								IOUtils.copyBytes(new ByteArrayInputStream(buf.toString().getBytes("UTF-8")), bos, 4096,
										true);
								bos.flush();
							}

							success = true;
						} finally {
							IOUtils.closeStream(bos);
						}
					}
				}
			}

			log.error("end handled datas.");
		} finally {

		}

		return success;
	}
}
