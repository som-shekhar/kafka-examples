package com.xorg.connector.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSinkTask extends SinkTask {

	private final static Logger LOGGER = LoggerFactory.getLogger(FileSinkTask.class);
	private final String VERSION = "1.0";
	private String logLocations;

	private static FileChannel fc;
	private static RandomAccessFile randomAccessFile;
	private FileLock fileLock;

	@Override
	public String version() {
		return VERSION;
	}

	@Override
	public void start(Map<String, String> props) {
		logLocations = props.get(FileSinkConnector.LOG_LOCATION_PROP);
		String fileName = logLocations + "/file-sink-" + UUID.randomUUID().toString();
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fc = randomAccessFile.getChannel();
		// acquiring lock
		LOGGER.info("Acquiring lock on filename:{}", fileName);
		try {
			fileLock = fc.lock();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// acquiring lock so this guy is exclusive writer

	}

	@Override
	public void put(Collection<SinkRecord> records) {

		// Open a file by under the given directory by adding the task id
		List<SinkRecord> collections = new ArrayList<>(records);

		for (SinkRecord rec : collections) {
			try {
				LOGGER.info("Receive key:{} and value:{}", rec.key(), rec.key());
				randomAccessFile.writeChars(rec.key() + ":" + rec.value());
				randomAccessFile.writeChars("\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	@Override
	public void stop() {
		try {
			fileLock.release();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
