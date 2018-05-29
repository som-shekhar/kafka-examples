package com.xorg.training.kafka.streams.processor;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xorg.training.kafka.util.UtilsConstant;

public class EMIPredictionStream {

	private static Logger logger = LoggerFactory
			.getLogger(EMIPredictionStream.class);

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "emi-prediction-stream");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
				Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
				Serdes.String().getClass());
		Topology topology = buildEMITopology();
		KafkaStreams streams = new KafkaStreams(topology, props);
		streams.start();
		logger.info("Stream started ...");
	}

	private static Topology buildEMITopology() {
		String sourceName = "SOURCE";
		String processorName = "EMI-PREDICTOR-PROCESSOR";
		String sinkName = "SINK";
		logger.info("Building topology ...");
		Topology topology = new Topology();
		topology.addSource(sourceName, UtilsConstant.CC_TRANSACTION_TOPIC);
		topology.addProcessor(processorName, new EMIConvertorSupplier(),
				sourceName);
		topology.addSink(sinkName, UtilsConstant.CC_EMI_PREDICT_TOPIC,
				processorName);
		logger.info("Describe topology - {}", topology.describe());
		return topology;
	}
}
