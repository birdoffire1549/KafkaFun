package com.firebirdcss.sandbox.kafka.kafka_fun.producer;

import java.util.Properties;
import java.util.Random;
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
	private static final String KAFKA_HOST_DEFAULT = "192.168.1.101";
	private static final String KAFKA_PORT_DEFAULT = "9092";
	private static final String TOPIC_DEFAULT = "my-topic";
	private static final int PUB_RATE_PER_SECOND_DEFAULT = 100000;
	private static final int PAYLOAD_SIZE_BYTES_DEFAULT = 8192;
	private static final boolean AUTO_PUBLISH_DEFAULT = false;
	
	private static boolean autoPublish = AUTO_PUBLISH_DEFAULT;
	private static int pubRatePerSecond = PUB_RATE_PER_SECOND_DEFAULT;
	
	private static String kafkaHost = KAFKA_HOST_DEFAULT;
	private static String kafkaPort = KAFKA_PORT_DEFAULT;
	private static String topic = TOPIC_DEFAULT;
	private static int payloadSizeBytes = PAYLOAD_SIZE_BYTES_DEFAULT;
	
	private static Producer<String, String> producer = null;
	
	/**
	 * APPLICATION MAIN: The main entry-point of the application.
	 * @param args
	 */
	public static void main(String[] args) {
		handleProgramArgs(args);
		init();
		System.out.println("Starting up with Kafka host of: " + kafkaHost);
		
		if (autoPublish) {
			System.out.println("Prepairing for auto publish...");
			String payload = generatePayload();
			boolean running = true;
			long messagesSentThisCycle = 0;
			long totalMessagesSent = 0;
			long firstSend = 0;
			System.out.println("Auto publishing begginning.");
			System.out.println("Sending messages that are '" + payload.length() + "' bytes in size.");
			while(running) {
				if (firstSend == 0) firstSend = System.currentTimeMillis();
				producer.send(new ProducerRecord<String, String>(topic, payload));
				totalMessagesSent ++;
				System.out.print("\rMessages Sent: " + totalMessagesSent);
				messagesSentThisCycle ++;
				if (messagesSentThisCycle >= pubRatePerSecond) {
					long sleepDiff = 0;
					if ((sleepDiff = 1000 - (System.currentTimeMillis() - firstSend)) > 0) {
						try {
							Thread.sleep(sleepDiff);
						} catch (InterruptedException ok) {
							running = false;
						}
					}
					firstSend = 0;
					messagesSentThisCycle = 0;
				}
			}
		} else {
			Scanner sc = new Scanner(System.in);
			String line = null;
			System.out.println("Enter text to send to consumer (type 'exit' to quit):");
			System.out.print("> ");
			while (!(line = sc.nextLine()).equals("exit")) {
				if (!line.equals("exit")) {
					producer.send(new ProducerRecord<String, String>(topic, line));
					System.out.print("> ");
				}
			}
			
			sc.close();
		}
		
		producer.close();
		System.out.println("\n ~ Application Ended ~");
	}
	
	/**
	 * TODO: This should generate a random payload of a specific size
	 * 
	 * @return Returns a payload as {@link String}
	 */
	private static String generatePayload() {
		char[] payload = new char[payloadSizeBytes];
		
		System.out.println("Building a '" + payloadSizeBytes + "' byte payload for sending...");
		Random r = new Random();
		for (int i = 0; i < payloadSizeBytes; i++) {
			payload[i] = (char) (r.nextInt(126 - 32) + 32);
		}
		System.out.println("Payload has been generated.");
		
		return String.copyValueOf(payload);
	}
	
	/**
	 * Initializes the application.
	 * 
	 * @param args
	 */
	private static void init() {
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaHost + ":" + kafkaPort);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
	}
	
	/**
	 * Parses Application arguments and setup the application to run as specified.
	 * 
	 * @param args
	 */
	private static void handleProgramArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
				case "-a":
				case "--autoPublish":
					autoPublish = true;
					break;
				case "-r":
				case "--rateOfPublish":
					try {
						pubRatePerSecond = Integer.parseInt(args[++i]);
					} catch (NumberFormatException bad) {
						printProgramUsage();
					}
					break;
				case "-H":
				case "--host":
					kafkaHost = args[++i];
					break;
				case "-s":
				case "--payloadSize":
					try {
						payloadSizeBytes = Integer.parseInt(args[++i]);
					} catch (NumberFormatException bad) {
						printProgramUsage();
					}
					break;
				case "-p":
				case "--port":
					kafkaPort = args[++i];
					break;
				case "-t":
				case "--topic":
					topic = args[++i];
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
		message.append("  [-a | --autoPublish]                  Autogenerates and publishes messages at the rate of 1 per second unless otherwise specified").append('\n');
		message.append("  [-H | --host] <hostname>              Specifies the hostname of the kafka broker").append('\n');
		message.append("  [-p | --port] <portNumber>            Specifies the port of the kafka broker").append('\n');
		message.append("  [-r | --rateOfPublish] <number>       Specifies the number of auto generated messages to publish a second").append('\n');
		message.append("  [-t | --topic] <topic>                The topic to which to publish messages").append('\n');
		message.append("  [-s | --payloadSize] <numberBytes>    The number of bytes the auto generated message should be in size").append('\n');
		message.append("  [-h | --help]                         Displays this program usage page.").append('\n');
		
		System.out.println(message);
		System.exit(1);
	}
}