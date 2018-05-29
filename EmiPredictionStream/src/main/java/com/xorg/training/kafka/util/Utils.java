package com.xorg.training.kafka.util;

import java.util.Random;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Utils implements UtilsConstant {

	private static int countPostfix = 1;
	private static JSONParser parser = new JSONParser();
	private static Random tarnsactionAmountGenerator = new Random(10000);
	@SuppressWarnings("unchecked")
	public static JSONObject getTransactionRecord() {

		JSONObject transactionValue = new JSONObject();
		String accountId = ACC_ID_PREFIX + countPostfix;
		String userName = ACC_NAME_PREFIX + countPostfix;
		transactionValue.put(ACC_ID, accountId);
		transactionValue.put(ACC_HOLDER_NAME, userName);
		transactionValue.put(ACC_AMOUNT,
				tarnsactionAmountGenerator.nextInt(10000));
		transactionValue.put(ACC_TRANSACTION_TIME, System.currentTimeMillis());
		countPostfix++;
		return transactionValue;
	}

	public static JSONObject parseTranscation(String strTranscation)
			throws ParseException {
		return (JSONObject) parser.parse(strTranscation);
	}
}
