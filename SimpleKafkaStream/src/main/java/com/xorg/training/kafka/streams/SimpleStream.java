package com.xorg.training.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleStream {

	private static Logger logger = LoggerFactory.getLogger(SimpleStream.class);
	
	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "transactions-simple-stream");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		logger.info("Creating Streams builder ...");
		StreamsBuilder builder = new StreamsBuilder();
		String sourceTopic = "transcations-input";
		String sinkTopic = "transcations-output";
		//Continuously reading from input topic  
		KStream<String, String> source = builder.stream(sourceTopic);
		//Output kafka topic
		source.to(sinkTopic);
		//Now build computation logic in stream base i.e. connected processor nodes define as Topology
		Topology topology = builder.build();
		logger.info("Topology details {}", topology.describe());
		final KafkaStreams streams = new KafkaStreams(topology, properties);
		
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				logger.info("Closing Stream ...");
				streams.close();
			}
		});
		
		try {
			logger.info("Starting Stream ...");
			streams.start();
		}catch (Exception e) {
			
		}
		
	}
}
