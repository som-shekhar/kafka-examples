package com.xorg.training.kafka.join;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

public class Joiner {

	public static void main(String[] args) {

		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique
		// in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "join-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "join-client");
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

		// streams of line
		KStream<Long, String> lines = builder.stream("employee");

		KStream<Long, Employee> mapValuesStream = lines.mapValues(line -> Parser.parseEmployee(line));
		KStream<String, String> mapStream = mapValuesStream
				.map((key, employee) -> new org.apache.kafka.streams.KeyValue<>(employee.getLocationID(),
						employee.getLastname() + ":" + employee.getFirstName()));

		KStream<String, String> locationStream = builder.stream("locations");
		KStream<String, String> modifiedLocStream = locationStream.mapValues(d -> Parser.parseLocation(d))
				.map((key, loc) -> new KeyValue<>(loc.getLocId(), loc.getLocName()));
		modifiedLocStream.to("loc-map");

		KTable<String, String> locTable = builder.table("loc-map");

		KStream<String, String> joined = mapStream.join(locTable, (emp, locName) -> emp + ":" + locName);

		joined.to("emp-loc-result");
		KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
		streams.start();

	}

}
