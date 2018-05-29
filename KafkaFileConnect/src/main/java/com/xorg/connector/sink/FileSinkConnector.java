package com.xorg.connector.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkConnector extends SinkConnector {
	private final static Logger log = LoggerFactory.getLogger(FileSinkConnector.class);

	private final String VERSION = "1.0";

	public final static String LOG_LOCATION_PROP = "log.location.dir";
	private String logLocations;

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define("FileSinkConnector", Type.STRING, "FileSinkConnector", Importance.HIGH, "WritingToFile");

	@Override
	public String version() {
		return VERSION;
	}

	@Override
	public void start(Map<String, String> props) {

		logLocations = props.get(LOG_LOCATION_PROP);
		if (null == logLocations || logLocations.isEmpty()) {
			throw new RuntimeException("Log location directory is either empty or not provided.");
		}

	}

	@Override
	public Class<? extends Task> taskClass() {
		return FileSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> configs = new ArrayList<>();
		// for each of the tasks add the configuratoin
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> props = new HashMap<String, String>();
			props.put(LOG_LOCATION_PROP, this.logLocations);
			configs.add(props);

		}
		return configs;
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

}
