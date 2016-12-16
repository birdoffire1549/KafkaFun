package com.firebirdcss.sandbox.kafka.kafka_fun.producer;

import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * This application acts as a sender for sending to a Kafka Topic.
 * 
 * @author Scott Griffis
 *
 */
public class KafkaSender {
	private static final String KAFKA_HOST = "192.168.1.101";
	private static final String KAFKA_PORT = "9092";
	private static final String TOPIC = "my-topic";
	
	/**
	 * APPLICATION MAIN: The main entry-point of the application.
	 * @param args
	 */
	public static void main(String[] args) {
		String host = (args != null && args.length > 0 ? args[0] : KAFKA_HOST);
		System.out.println("Starting up with Kafka host of: " + host);
		
		Properties props = new Properties();
		props.put("bootstrap.servers", host + ":" + KAFKA_PORT);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		
		Scanner sc = new Scanner(System.in);
		String line = null;
		System.out.println("Enter text to send to consumer (type 'exit' to quit):");
		System.out.print("> ");
		while (!(line = sc.nextLine()).equals("exit")) {
			if (!line.equals("exit")) {
				producer.send(new ProducerRecord<String, String>(TOPIC, line));
				System.out.print("> ");
			}
		}
		
		sc.close();
		producer.close();
		System.out.println("\n ~ Application Ended ~");
	}
}