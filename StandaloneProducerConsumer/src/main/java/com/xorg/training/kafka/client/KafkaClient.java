package com.xorg.training.kafka.client;

import com.xorg.training.kafka.consumer.ConsumerStandalone;
import com.xorg.training.kafka.producer.ProducerStandalone;

public class KafkaClient {

	public static void main(String[] args) {

		// Starting Kafka producer
		Thread producerThread = new Thread(new Runnable() {

			public void run() {
				ProducerStandalone.startProducer();
			}
		});
		producerThread.start();

		// Starting Kafka consumer
		Thread consumer = new Thread(new Runnable() {

			public void run() {
				ConsumerStandalone.startConsumer();
			}
		});
		consumer.start();
	}
}
