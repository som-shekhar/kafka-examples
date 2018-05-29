package com.xorg.connector.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSourceTask extends SourceTask {

	private final static Logger LOGGER = LoggerFactory.getLogger(FileSourceTask.class);

	private String dataDir;

	private FileLock fileLock;

	public static final String FILENAME_FIELD = "filename";
	public static final String POSITION_FIELD = "position";

	private String topic;

	private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

	@Override
	public String version() {
		return FileSourceConnector.VERSION;
	}

	@Override
	public void start(Map<String, String> props) {

		dataDir = props.get(FileSourceConnector.DATA_DIRECTORY);
		this.topic = props.get(FileSourceConnector.TOPIC_CONFIG);
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {

		File folder = new File(dataDir);
		File[] listOfFiles = folder.listFiles();
		List<SourceRecord> recordList = null;
		synchronized (this) {
			for (File file : listOfFiles) {

				if (file.isFile() && file.getName().startsWith("file-source") && !file.getName().endsWith("done")) {

					try {
						FileReader fr = new FileReader(file.getName());
						BufferedReader br = new BufferedReader(fr);

						String sCurrentLine;

						while ((sCurrentLine = br.readLine()) != null) {
							SourceRecord record = new SourceRecord(offsetKey(file.getName()), offsetValue(1l), topic, VALUE_SCHEMA,
									sCurrentLine);
							recordList.add(record);
						}

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		if (null != recordList && !recordList.isEmpty()) {
			return recordList;
		}
		return recordList;
	}

	private List<SourceRecord> read(String fileName) throws IOException {
		RandomAccessFile accessFile = acquireLock(fileName);
		List<SourceRecord> records = new ArrayList<SourceRecord>();

		String line;
		while ((line = accessFile.readLine()) != null) {
			SourceRecord record = new SourceRecord(offsetKey(fileName), offsetValue(1l), topic, VALUE_SCHEMA, line);
			records.add(record);
		}
		// move the file name to done and relaese the lock
		// since it is just a sampel program we can leave it here
		releaseLock();
		return records;

	}

	private RandomAccessFile acquireLock(String fileName) throws IOException {
		RandomAccessFile randomAccessFile = null;
		try {
			randomAccessFile = new RandomAccessFile(fileName, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileChannel fc = randomAccessFile.getChannel();
		// acquiring lock
		LOGGER.info("Acquiring lock on filename:{}", fileName);
		try {
			fileLock = fc.lock();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return randomAccessFile;

	}

	private void releaseLock() throws IOException {
		fileLock.release();
	}

	@Override
	public void stop() {

	}

	private Map<String, String> offsetKey(String filename) {
		return Collections.singletonMap(FILENAME_FIELD, filename);
	}

	private Map<String, Long> offsetValue(Long pos) {
		return Collections.singletonMap(POSITION_FIELD, pos);
	}

}
