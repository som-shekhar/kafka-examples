package com.xorg.training.kafka.simulators;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xorg.training.kafka.util.Utils;
import com.xorg.training.kafka.util.UtilsConstant;

public class CreditCardTransactionsProducer {
	private static Logger logger = LoggerFactory
			.getLogger(CreditCardTransactionsProducer.class);
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

			while (true) {
				JSONObject transactionValue = Utils.getTransactionRecord();
				ProducerRecord<String, String> record = new ProducerRecord<String, String>(
						UtilsConstant.CC_TRANSACTION_TOPIC,
						transactionValue.get("AccountId").toString(),
						transactionValue.toJSONString());
				logger.info("Producing key {} value {}", record.key(),
						record.value());
				logger.info("Complete record info {}", record);

				producer.send(record);
				Thread.sleep(1000);
				logger.info(
						"------------ Record inserted to kafka -----------");
			}
		} catch (Exception e) {
			logger.error("Error while producing.", e);
		} finally {
			producer.close();
		}
	}

}
