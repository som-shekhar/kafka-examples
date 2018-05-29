package com.xorg.training.kafka.streams.processor;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xorg.training.kafka.util.Utils;
import com.xorg.training.kafka.util.UtilsConstant;

public class EMIConvertorProcessor extends AbstractProcessor<String, String> {

	private static Logger logger = LoggerFactory
			.getLogger(EMIConvertorProcessor.class);

	public void process(String key, String value) {

		try {
			JSONObject transaction = Utils.parseTranscation(value);
			long transactionAmount = (Long) transaction
					.get(UtilsConstant.ACC_AMOUNT);
			if (transactionAmount > 2000) {
				logger.info(
						"User might convert this transaction to EMI. Amount {}",
						transactionAmount);
				context().forward(key, value);
			} else {
				logger.info("No need to convert into EMI. Amount {}",
						transactionAmount);
			}

		} catch (ParseException e) {
			logger.error("Error while processing record. ", e);
		}
	}

}
