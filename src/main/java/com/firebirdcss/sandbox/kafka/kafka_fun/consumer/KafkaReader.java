package com.firebirdcss.sandbox.kafka.kafka_fun.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Sample Reader application for reading from a Kafka Topic.
 * 
 * @author Scott Griffis
 *
 */
public class KafkaReader {
	private static final String KAFKA_HOST = "192.168.1.101";
	private static final String KAFKA_PORT = "9092";
	private static final String TOPIC = "my-topic";
	private static final String GROUP_SEQ = "1";
	
	/**
	 * APPLICATION MAIN: Main entry-point for the application.
	 * 
	 * @param args - Arguments as {@link String}[]
	 */
	public static void main(String[] args) {
		String group = (args != null && args.length > 0 ? args[0] : GROUP_SEQ);
		String host = (args != null && args.length > 1 ? args[1] : KAFKA_HOST);
		
		System.out.println("Starting up with Kafka host of: " + host);
		
		Properties props = new Properties();
		props.put("bootstrap.servers", host + ":" + KAFKA_PORT);
		props.put("group.id", "group" + group);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		System.out.println("Attempting to subscribe to assigned topic...");
		consumer.subscribe(Arrays.asList(TOPIC));
		System.out.println("Topic subscription complete.");
		
		boolean running = true;
		System.out.println("Kafka Reader has started. Results will display as data is provided to the topic:");
		
		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf(": Record Received with offset = %d and value = %s%n", record.offset(), record.value());
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException ok) {
				running = false;
			}
		}
		
		consumer.close();
		System.out.println("\n~ Application Ended ~");
	}
}