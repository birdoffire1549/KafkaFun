package com.firebirdcss.sandbox.kafka.kafka_fun;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This class acts as a kind of utility class, whose job is to
 * read data from one topic, and then place it onto another topic.
 * With a minor modification data can be manipulated while being moved from one topic 
 * to another.
 * 
 * @author Scott Griffis
 *
 */
public class KafkaTopicCopier {
	private static final String KAFKA_HOST = "192.168.1.214";
	private static final String KAFKA_PORT = "9092";
	private static final String TOPIC_IN = "CUSTOM-IN-TOPIC";
	private static final String TOPIC_OUT = "NEW-OUT-TOPIC";
	private static final String GROUP_NAME = "group1";
	
	private static KafkaConsumer<String, String> consumer = null;
	private static Producer<String, String> producer = null;
	
	/**
	 * Used to initialize the application.
	 */
	private static void init() {
		/* INIT CONSUMER */
		Properties propsC = new Properties();
		propsC.put("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
		propsC.put("group.id", GROUP_NAME);
		propsC.put("enable.auto.commit", "true");
		propsC.put("auto.commit.interval.ms", "500");
		propsC.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<>(propsC);
		
		/* INIT PRODUCER */
		Properties propsP = new Properties();
		propsP.put("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
		propsP.put("acks", "all");
		propsP.put("retries", 0);
		propsP.put("batch.size", 16384);
		propsP.put("linger.ms", 1);
		propsP.put("buffer.memory", 33554432);
		propsP.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		propsP.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(propsP);
	}
	
	/**
	 * Application's main method.
	 * 
	 * @param args - Not used for this application.
	 */
	public static void main(String[] args) {
		init();
		
		System.out.println("Attempting to subscribe to assigned topic...");
		consumer.subscribe(Arrays.asList(new String[]{TOPIC_IN}));
		System.out.println("Topic subscription complete.");
		
		boolean running = true;
		System.out.println("Kafka Reader has started.");
		
		long recordCount = 0;
		System.out.println("Messages Copied:");
		System.out.print(recordCount);
		
		while (running) {
			ConsumerRecords<String, String> records = consumer.poll(100/*TimeOutMillis*/);
			if (records.isEmpty()) {
				try {	
					Thread.sleep(500);
				} catch (InterruptedException ok) {
					running = false;
				}
			} else {
				for (ConsumerRecord<String, String> record : records) {
					producer.send(new ProducerRecord<String, String>(TOPIC_OUT, record.value().replaceAll("3947", "600")));
					recordCount ++;
				}
			}
			System.out.print("\r" + recordCount);
		}
		
		producer.close();
		consumer.close();
		System.out.println("\n~ Application Ended ~");
	}
}
