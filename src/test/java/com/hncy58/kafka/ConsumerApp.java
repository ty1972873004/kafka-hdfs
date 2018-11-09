package com.hncy58.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerApp {

	static int FETCH_SIZE = 128;
	static int SLEEP_SECONDS = 5;

	private static Long total_msg = 0L;

	private static boolean PRINT_RECEIVED_DATA = false;

	public static void main(String[] args) {

		Properties props = new Properties();
		if (args.length > 0) {
			props.put("bootstrap.servers", args[0]);
		} else {
			props.put("bootstrap.servers", "192.168.144.128:9092");
		}

		if (args.length > 1) {
			FETCH_SIZE = Integer.parseInt(args[1].trim());
		}

		if (args.length > 2) {
			PRINT_RECEIVED_DATA = Boolean.valueOf(args[2].trim());
		}
		
		if (args.length > 3) {
			SLEEP_SECONDS = Integer.parseInt(args[3].trim());
		}

		props.put("group.id", "my-group-id");

		// props.put("enable.auto.commit", "true");
		// props.put("auto.commit.interval.ms", "1000");
		props.put("enable.auto.commit", "false");

		// 设置只读事务消息
		props.put("isolation.level", "read_committed");

		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList(ProducerApp.TOPIC_NAME));

		final int minBatchSize = 50000;
		List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

		try {
			long start = System.currentTimeMillis();
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(FETCH_SIZE);
				if (records.count() > 0) {
					System.out.println("current polled " + records.count() + " records.");
					total_msg += records.count();
					System.out.println("total polled " + total_msg + " records.");
					for (ConsumerRecord<String, String> record : records) {
						buffer.add(record);
					}
					if (buffer.size() >= minBatchSize) {
						System.out.println(System.currentTimeMillis() - start + "ms.");
						start = System.currentTimeMillis();
						handle(buffer);
						consumer.commitSync();
						buffer.clear();
						
					} else {
						System.out.println("current buffer remains " + buffer.size() + " records.");
					}
				} else {
					System.out.println("no data to poll, sleep " + SLEEP_SECONDS + " s.");
					Thread.sleep(SLEEP_SECONDS * 1000);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}

	private static void handle(List<ConsumerRecord<String, String>> buffer) {
		System.out.println("start to handle datas -> " + buffer.size());
		if (PRINT_RECEIVED_DATA) {
			buffer.forEach(record -> {
				System.out.printf("key:%s, offset:%s, partition:%s, ts:%s, value:%s\n", record.key(), record.offset(),
						record.partition(), record.timestamp(), record.value());
			});
		}
		System.out.println("end handled datas.");
	}
}