package com.xorg.training.kafka.streams;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class WordCountDSL {

	public static void main(String[] args) {

		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique
		// in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-dsl");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-example-dsl");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Specify default (de)serializers for record keys and for record
		// values.
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		StreamsBuilder builder = new StreamsBuilder();
		// create a stream on comedies
		KStream<String, String> iStream = builder.stream("comedies_1");
		/**
		 * key = null value = string
		 */
		KStream<String, String> flatMapStream = iStream.flatMapValues(line -> Arrays.asList(line.split("\\W+")));

		flatMapStream.to("flat-map-t");

		KGroupedStream<String, String> groupedStream = flatMapStream.groupBy((key, word) -> word);

		groupedStream.count().toStream().to("wc-counts-dsl", Produced.with(Serdes.String(), Serdes.Long()));
		// KTable<String, Long> countsTable = groupedStream.count();
		// countsTable.toStream().to("wc-counts-dsl",
		// Produced.with(Serdes.String(), Serdes.Long()));

		KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.start();
	}

}
