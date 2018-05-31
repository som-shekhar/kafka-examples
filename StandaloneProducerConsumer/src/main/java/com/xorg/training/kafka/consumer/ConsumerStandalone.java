package com.xorg.training.kafka.consumer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * It consumes the data from kafka topic
 */
public class ConsumerStandalone {

	private static Logger logger = LoggerFactory
			.getLogger(ConsumerStandalone.class);
	private static KafkaConsumer<String, String> consumer;

	public static void main(String[] args) {
		startConsumer();
	}

	public static void startConsumer() {
		try {
			Properties configs = new Properties();
			// Brokers IP and ports
			configs.setProperty("bootstrap.servers", "127.0.0.1:9092");
			// topic key Deserializer class
			configs.setProperty("key.deserializer",
					StringDeserializer.class.getName());
			// topic value Deserializer class
			configs.setProperty("value.deserializer",
					StringDeserializer.class.getName());
			// can run multiple consumer for the same group id
			configs.setProperty("group.id", "consumerStandalone");

			// topic name
			String topicName = "transcations";

			// kafka topic to subscribe for consuming data
			ArrayList<String> topics = new ArrayList<String>();
			topics.add(topicName);

			// Creating Kafka Consumer
			consumer = new KafkaConsumer<String, String>(configs);
			// subscribe the consumer to the topic
			consumer.subscribe(topics);

			ConsumerRecords<String, String> consumerRecords;
			logger.info("Starting consuming from kafka...");
			while (true) {
				// Polling data with timeout 2000 ms
				consumerRecords = consumer.poll(2000);
				for (ConsumerRecord<String, String> record : consumerRecords) {
					logger.info("Received Key {} ,value {},from topic:{} from partition:{} from offset:{}", record.key(),
							record.value(), record.topic(), record.partition(), record.offset());
					logger.info("Complete record {} ", record);
					logger.info("------------ record read --------------");
				}
			}
		} catch (Exception e) {
			logger.error("Error while consuming.", e);
		} finally {
			consumer.close();
		}
	}
}
