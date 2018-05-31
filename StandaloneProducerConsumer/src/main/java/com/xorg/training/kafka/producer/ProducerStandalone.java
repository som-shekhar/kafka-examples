package com.xorg.training.kafka.producer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerStandalone {
	private static Logger logger = LoggerFactory
			.getLogger(ProducerStandalone.class);
	private static Producer<String, String> producer;

	public static void main(String[] args) {
		startProducer();
	}

	public static void startProducer() {
		try {
			Properties configs = new Properties();
			// Broker IPs
			configs.setProperty("bootstrap.servers", "127.0.0.1:9092");
			// topic key Serializer
			configs.setProperty("key.serializer",
					StringSerializer.class.getName());
			// topic value Serializer
			configs.setProperty("value.serializer",
					StringSerializer.class.getName());

			producer = new KafkaProducer<String, String>(configs);
			// Topic name
			String topic = "transcations";

			/***
			 * form the record call the producer to send the data to the top
			 */
			while (true) {
				JSONObject transactionValue = getTransactionRecord();
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(
						topic, transactionValue.get("AccountId").toString(),
						transactionValue.toJSONString());
				logger.info("Producing key {} value {}", record.key(),
						record.value());
				logger.info("Complete record info {}", record);

				producer.send(record);
				Thread.sleep(2000);
				logger.info(
						"------------ Record inserted to kafka -----------");
			}
		} catch (Exception e) {
			logger.error("Error while producing.", e);
		} finally {
			producer.close();
		}
	}

	private static int countPostfix = 1;

	@SuppressWarnings("unchecked")
	private static JSONObject getTransactionRecord() {
		String accountIdPrefix = "BANK_XYZ_";
		String accountNamePrefix = "USER_";
		Random tarnsactionAmountGenerator = new Random(1000);
		JSONObject transactionValue = new JSONObject();
		String accountId = accountIdPrefix + countPostfix;
		String userName = accountNamePrefix + countPostfix;
		transactionValue.put("AccountId", accountId);
		transactionValue.put("AccountHolderName", userName);
		transactionValue.put("Amount",
				tarnsactionAmountGenerator.nextInt(100000));
		transactionValue.put("transactionTime", System.currentTimeMillis());
		countPostfix++;
		return transactionValue;
	}
}
