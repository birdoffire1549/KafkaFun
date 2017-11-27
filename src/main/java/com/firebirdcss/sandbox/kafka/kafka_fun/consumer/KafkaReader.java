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
	private static final String KAFKA_HOST_DEFAULT = "kafka01.domain.com";
	private static final String KAFKA_PORT_DEFAULT = "9092";
	private static final String[] TOPICS_DEFAULT = new String[] {"TOPIC-1"};
	private static final String GROUP_NAME_DEFAULT = "funGroup";
	
	private static boolean showMessages = false;
	
	private static String kafkaHost = KAFKA_HOST_DEFAULT;
	private static String kafkaPort = KAFKA_PORT_DEFAULT;
	private static String groupName = GROUP_NAME_DEFAULT;
	private static String[] topics = TOPICS_DEFAULT;
	
	private static KafkaConsumer<String, String> consumer = null;
	
	/**
	 * APPLICATION MAIN: Main entry-point for the application.
	 * 
	 * @param args - Arguments as {@link String}[]
	 */
	public static void main(String[] args) {
		handleProgramArgs(args);
		init();
		
		System.out.println("Starting up with Kafka host of: " + kafkaHost + " and a port of: " + kafkaPort);
		
		System.out.println("Attempting to subscribe to assigned topic(s)...");
		consumer.subscribe(Arrays.asList(topics));
//		List<TopicPartition> list = new ArrayList<>();
//		list.add(new TopicPartition("AGI-TOPIC-3947", 2));
//		consumer.assign(list);
		System.out.println("Topic subscription complete.");
		
		boolean running = true;
		System.out.println("Kafka Reader has started. Results will display as data is provided to the topic:");
		
		long recordCount = 0;
		if (!showMessages) {
			System.out.println("Messages Consumed:");
			System.out.print(recordCount);
		}
		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(2000/*TimeOutMillis*/);
			if (records.isEmpty()) {
				try {	
					Thread.sleep(500);
				} catch (InterruptedException ok) {
					running = false;
				}
			} else {
				for (ConsumerRecord<String, String> record : records) {
					recordCount ++;
					if (showMessages) {
						System.out.printf(": Record Received with offset = %d and value = %s%n", record.offset(), record.value());
					}
				}
			}
			if (!showMessages) System.out.print("\r" + recordCount);
		}
		consumer.close();
		System.out.println("\n~ Application Ended ~");
	}
	
	/**
	 * Initializes the application.
	 * 
	 * @param args
	 */
	private static void init() {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaHost + ":" + kafkaPort);
		props.put("group.id", groupName);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "500");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<>(props);
	}
	
	/**
	 * Parses Application arguments and setup the application to run as specified.
	 * 
	 * @param args
	 */
	private static void handleProgramArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-s":
				case "--showMessages":
					showMessages = true;
					break;
				case "-H":
				case "--host":
					kafkaHost = args[++i];
					break;
				case "-p":
				case "--port":
					kafkaPort = args[++i];
					break;
				case "-g":
				case "--group":
					groupName = args[++i];
					break;
				case "-t":
				case "--topics":
					topics = args[++i].split(","); /* CSV list of topics */
					break;
				case "-h":
				case "--help":
				default:
					printProgramUsage();
					break;
			}
		}
	}
	
	/**
	 * Prints the program usage to the screen for the user to see.
	 * 
	 */
	private static void printProgramUsage() {
		StringBuilder message = new StringBuilder();
		message.append("program usage: ").append("program [options]").append('\n');
		message.append("Options:\n");
		message.append("  [-s | --showMessages]                 Causes every messages received to be displayed to console").append('\n');
		message.append("  [-H | --host] <hostname>              Specifies the hostname of the kafka broker").append('\n');
		message.append("  [-p | --port] <portnumber>            Specifies the port of the kafka broker").append('\n');
		message.append("  [-g | --group] <groupname>            Specifies the consumer group to which the consumer belongs").append('\n');
		message.append("  [-t | --topics] <topic1,topic2...>    A CSV list of topics to subscribe to").append('\n');
		message.append("  [-h | --help]                         Displays this program usage page.").append('\n');
		
		System.out.println(message);
		System.exit(1);
	}
}