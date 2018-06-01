package com.xorg.training.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountStreamProcessor implements Processor<byte[], byte[]> {

	// one time call, want to do initializing something in this method

	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountStreamProcessor.class);
	private ProcessorContext context;
	private KeyValueStore<String, Long> kvStore;

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
		this.kvStore = (KeyValueStore<String, Long>) this.context.getStateStore("counts");

		this.context.schedule(10000, PunctuationType.WALL_CLOCK_TIME, new Punctuator() {

			@Override
			public void punctuate(long timestamp) {
				KeyValueIterator<String, Long> allData = ((KeyValueStore<String, Long>) context.getStateStore("counts")).all();

				while (allData.hasNext()) {
					KeyValue<String, Long> data = allData.next();
					String word = data.key;
					Long value = data.value;
					context.forward(word.getBytes(), Long.toString(value).getBytes());
				}
				allData.close();

			}
		});

	}

	/**
	 * This method will be called for every data that lands up in the topc
	 */
	@Override
	public void process(byte[] key, byte[] value) {
		if (null == key) {
			LOGGER.error("Received null key");
			return;
		}
		if (null == value) {
			LOGGER.error("Received null value");
			return;
		}
		// value will be the text
		String str = new String(value);
		LOGGER.info("Recevied data:{} from topic:{} from partition:{}", str, this.context.topic(), this.context.partition());

		String[] words = str.split("\\W+");

		for (String w : words) {
			if (w.length() > 0) {
				Long count = this.kvStore.get(w);
				if (null == count) {
					this.kvStore.put(w, 1l);
				}
				else {
					this.kvStore.put(w, count + 1);
				}
			}
		}

	}

	/**
	 * punctuate method can be scheduled by the kafka
	 */
	@Override
	public void punctuate(long timestamp) {
		KeyValueIterator<String, Long> allData = this.kvStore.all();

		while (allData.hasNext()) {
			KeyValue<String, Long> data = allData.next();
			String word = data.key;
			long value = data.value;
			this.context.forward(word, value);
		}
		allData.close();

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
