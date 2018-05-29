package com.xorg.connector.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class FileSourceConnector extends SourceConnector {

	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define("FileSourceConnector", Type.STRING, "FileSourceConnector", Importance.HIGH, "ReadingFromDirectory");

	public final static String DATA_DIRECTORY = "data.dir";
	public static final String TOPIC_CONFIG = "topic";
	private String dataDir;

	private String topic;

	public final static String VERSION = "1.0";

	@Override
	public String version() {

		return VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
		dataDir = props.get(DATA_DIRECTORY);
		if (null == dataDir || dataDir.isEmpty()) {
			throw new RuntimeException("Property data.dir is either null or not defined.");
		}
		topic = props.get(TOPIC_CONFIG);
		if (null == topic || topic.isEmpty()) {
			throw new RuntimeException("Property topic is either null or not defined.");
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return FileSourceTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> configs = new ArrayList<>();
		// for each of the tasks add the configuratoin
		for (int i = 0; i < maxTasks; i++) {
			Map<String, String> props = new HashMap<String, String>();
			props.put(DATA_DIRECTORY, this.dataDir);
			props.put(TOPIC_CONFIG, this.topic);
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
