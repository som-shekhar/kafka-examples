package com.xorg.training.kafka.streams.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class EMIConvertorSupplier implements ProcessorSupplier<String, String> {

	public Processor<String, String> get() {

		return new EMIConvertorProcessor();
	}

}
