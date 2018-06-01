package com.xorg.training.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;

public class WordCountMain {

	public static void main(String[] args) {

		final Properties streamsConfiguration = new Properties();
		// Give the Streams application a unique name. The name must be unique
		// in the Kafka cluster
		// against which the application is run.
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wc-example");
		streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wc-client");
		// Where to find Kafka broker(s).
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Specify default (de)serializers for record keys and for record
		// values.
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
		// Records should be flushed every 10 seconds. This is less than the
		// default
		// in order to keep this example interactive.
		streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
		// For illustrative purposes we disable record caches
		streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

		Topology builder = new Topology();
		builder.addSource("SOURCE", "comedies");

		builder.addProcessor("WordCountProcessor", new ProcessorSupplier<byte[], byte[]>() {

			@Override
			public Processor<byte[], byte[]> get() {
				return new WordCountStreamProcessor();
			}
		}, "SOURCE");

		builder.addSink("SINK", "wordcounts", "WordCountProcessor");

		builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("counts"), Serdes.String(), Serdes.Long()),
				"WordCountProcessor");
		builder.connectProcessorAndStateStores("WordCountProcessor", "counts");
		KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
		streams.start();

	}

}
