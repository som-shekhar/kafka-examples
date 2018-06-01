package com.xorg.training.kafka.streams;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileProducer {

	private static Logger LOGGER = LoggerFactory
			.getLogger(FileProducer.class);
	private static Producer<byte[], byte[]> producer;

	public static void main(String[] args) {
		if (args.length > 0)
			startProducer(args);
		else
			throw new RuntimeException("Expect one arguement that represents filename");
	}

	public static void startProducer(String[] args) {
		try {
			Properties configs = new Properties();
			// Broker IPs
			configs.setProperty("bootstrap.servers", "127.0.0.1:9092");
			// topic key Serializer
			configs.setProperty("key.serializer",
					ByteArraySerializer.class.getName());
			// topic value Serializer
			configs.setProperty("value.serializer",
					ByteArraySerializer.class.getName());

			producer = new KafkaProducer<byte[], byte[]>(configs);
			// Topic name
			String topic = "comedies";

			/***
			 * form the record call the producer to send the data to the top
			 */
			String fileName = args[0];

			String line;

			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileName))));
			String key = "myKey";

			while (null != (line = reader.readLine())) {
				LOGGER.info("Received line:{}", line);
				ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, key.getBytes(), line.getBytes());
				producer.send(record);
			}

		} catch (Exception e) {
			LOGGER.error("Error while producing.", e);
		} finally {
			producer.close();
		}
	}

	private static int countPostfix = 1;

}
