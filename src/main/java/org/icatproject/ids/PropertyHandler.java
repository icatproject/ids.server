package org.icatproject.ids;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonReader;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.IdsBean.CallType;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Load the properties specified in the properties file ids.properties.
 */
public class PropertyHandler {

	private static PropertyHandler instance = null;
	private static final Logger logger = LoggerFactory.getLogger(PropertyHandler.class);

	public synchronized static PropertyHandler getInstance() {
		if (instance == null) {
			instance = new PropertyHandler();
		}
		return instance;
	}

	public static Logger getLogger() {
		return logger;
	}

	private ArchiveStorageInterface archiveStorage;
	private Path cacheDir;
	private boolean enableWrite;
	private Path filesCheckErrorLog;
	private int filesCheckGapMillis;
	private Path filesCheckLastIdFile;
	private int filesCheckParallelCount;
	private ICAT icatService;
	private long linkLifetimeMillis;

	private MainStorageInterface mainStorage;

	private int preparedCount;

	private long processQueueIntervalSeconds;
	private List<String> reader;
	private boolean readOnly;
	private Set<String> rootUserNames;
	private long sizeCheckIntervalMillis;
	private long startArchivingLevel;
	private long stopArchivingLevel;
	private StorageUnit storageUnit;
	private long delayDatasetWrites;
	private long delayDatafileOperations;
	private ZipMapperInterface zipMapper;
	private int tidyBlockSize;
	private String key;
	private int maxIdsInQuery;
	private String icatUrl;
	private Integer maxEntities;
	private String jmsTopicConnectionFactory;
	private Set<CallType> logSet = new HashSet<>();
	private org.icatproject.icat.client.ICAT restIcat;
	private boolean useReaderForPerformance;

	@SuppressWarnings("unchecked")
	private PropertyHandler() {

		CheckedProperties props = new CheckedProperties();

		try {
			props.loadFromResource("run.properties");
			logger.info("Property file run.properties loaded");

			icatUrl = ICATGetter.getCleanUrl(props.getString("icat.url"));
			try {
				restIcat = new org.icatproject.icat.client.ICAT(icatUrl);
			} catch (URISyntaxException e) {
				abort(e.getMessage());
			}

			preparedCount = props.getPositiveInt("preparedCount");
			processQueueIntervalSeconds = props.getPositiveLong("processQueueIntervalSeconds");
			rootUserNames = new HashSet<>(Arrays.asList(props.getString("rootUserNames").trim().split("\\s+")));

			reader = Arrays.asList(props.getString("reader").trim().split("\\s+"));
			if (reader.size() % 2 != 1) {
				throw new IllegalStateException("reader must have an odd number of words");
			}

			readOnly = props.getBoolean("readOnly", false);
			if (props.has("enableWrite")) {
				enableWrite = props.getBoolean("enableWrite");
			} else {
				enableWrite = ! readOnly;
			}

			sizeCheckIntervalMillis = props.getPositiveInt("sizeCheckIntervalSeconds") * 1000L;

			if (props.has("key")) {
				key = props.getString("key");
			}

			try {
				Class<ZipMapperInterface> klass = (Class<ZipMapperInterface>) Class
						.forName(props.getString("plugin.zipMapper.class"));
				zipMapper = klass.getConstructor().newInstance();
				logger.debug("ZipMapper initialised");

			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				abort(e.getClass() + " " + e.getMessage());
			}

			// Now get simple properties to pass to the plugins
			Properties simpleProps = new Properties();
			try (InputStream is = getClass().getClassLoader().getResourceAsStream("run.properties")) {
				simpleProps.load(is);
			} catch (IOException e) {
				abort(e.getClass() + " " + e.getMessage());
			}

			try {
				Class<MainStorageInterface> klass = (Class<MainStorageInterface>) Class
						.forName(props.getString("plugin.main.class"));
				mainStorage = klass.getConstructor(Properties.class).newInstance(simpleProps);
				logger.debug("mainStorage initialised");
			} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e) {
				logger.error("Plugin failed...", e);
				abort(e.getClass() + " " + e.getMessage());
			}

			if (!props.has("plugin.archive.class")) {
				logger.info("Property plugin.archive.class not set, single storage enabled.");
			} else {
				try {
					Class<ArchiveStorageInterface> klass = (Class<ArchiveStorageInterface>) Class
							.forName(props.getString("plugin.archive.class"));
					archiveStorage = klass.getConstructor(Properties.class).newInstance(simpleProps);
					logger.debug("archiveStorage initialised");
				} catch (ClassNotFoundException | InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException | NoSuchMethodException
						| SecurityException e) {
					logger.error("Plugin failed...", e);
					abort(e.getClass() + " " + e.getMessage());
				}
				startArchivingLevel = props.getPositiveLong("startArchivingLevel1024bytes") * 1024;
				stopArchivingLevel = props.getPositiveLong("stopArchivingLevel1024bytes") * 1024;
				if (stopArchivingLevel >= startArchivingLevel) {
					abort("startArchivingLevel1024bytes must be greater than stopArchivingLevel1024bytes");
				}

				try {
					String storageUnitName = props.getString("storageUnit");
					storageUnit = StorageUnit.valueOf(storageUnitName.toUpperCase());
				} catch (IllegalArgumentException e) {
					List<String> vs = new ArrayList<>();
					for (StorageUnit s : StorageUnit.values()) {
						vs.add(s.name());
					}
					abort("storageUnit value " + props.getString("storageUnit") + " must be taken from " + vs);
				}
				if (storageUnit == StorageUnit.DATASET) {
					delayDatasetWrites = props.getPositiveLong("delayDatasetWritesSeconds");
				} else if (storageUnit == StorageUnit.DATAFILE) {
					delayDatafileOperations = props.getPositiveLong("delayDatafileOperationsSeconds");
				}
				tidyBlockSize = props.getPositiveInt("tidyBlockSize");
			}

			try {
				cacheDir = props.getFile("cache.dir").getCanonicalFile().toPath();
			} catch (IOException e) {
				abort("IOException " + e.getMessage());
			}
			if (!Files.isDirectory(cacheDir)) {
				abort(cacheDir + " must be an existing directory");
			}

			filesCheckParallelCount = props.getNonNegativeInt("filesCheck.parallelCount");
			if (filesCheckParallelCount > 0) {
				filesCheckGapMillis = props.getPositiveInt("filesCheck.gapSeconds") * 1000;
				filesCheckLastIdFile = props.getFile("filesCheck.lastIdFile").toPath();
				if (!Files.exists(filesCheckLastIdFile.getParent())) {
					abort("Directory for " + filesCheckLastIdFile + " does not exist");
				}
				filesCheckErrorLog = props.getFile("filesCheck.errorLog").toPath();
				if (!Files.exists(filesCheckErrorLog.getParent())) {
					abort("Directory for " + filesCheckErrorLog + " does not exist");
				}
			}

			linkLifetimeMillis = props.getNonNegativeLong("linkLifetimeSeconds") * 1000L;

			maxIdsInQuery = props.getPositiveInt("maxIdsInQuery");

			/* JMS stuff */
			jmsTopicConnectionFactory = props.getString("jms.topicConnectionFactory",
					"java:comp/DefaultJMSConnectionFactory");

			/* Call logging categories */
			if (props.has("log.list")) {
				for (String callTypeString : props.getString("log.list").split("\\s+")) {
					try {
						logSet.add(CallType.valueOf(callTypeString.toUpperCase()));
					} catch (IllegalArgumentException e) {
						abort("Value " + callTypeString + " in log.list must be chosen from "
								+ Arrays.asList(CallType.values()));
					}
				}
				logger.info("log.list: " + logSet);
			} else {
				logger.info("'log.list' entry not present so no JMS call logging will be performed");
			}

			useReaderForPerformance = props.getBoolean("useReaderForPerformance", false);

		} catch (CheckedPropertyException e) {
			abort(e.getMessage());
		}
	}

	private void abort(String msg) {
		logger.error(msg);
		logger.error("IllegalStateException being thrown");
		throw new IllegalStateException(msg);
	}

	public ArchiveStorageInterface getArchiveStorage() {
		return archiveStorage;
	}

	public Path getCacheDir() {
		return cacheDir;
	}

	public boolean getEnableWrite() {
		return enableWrite;
	}

	public Path getFilesCheckErrorLog() {
		return filesCheckErrorLog;
	}

	public long getFilesCheckGapMillis() {
		return filesCheckGapMillis;
	}

	public Path getFilesCheckLastIdFile() {
		return filesCheckLastIdFile;
	}

	public int getFilesCheckParallelCount() {
		return filesCheckParallelCount;
	}

	public synchronized ICAT getIcatService() {
		// Keep trying every 10 seconds to connect to ICAT. Each failure
		// will produce an error message.
		while (icatService == null) {
			try {
				icatService = ICATGetter.getService(icatUrl);
			} catch (IcatException_Exception e) {
				String msg = "Problem finding ICAT API version at " + icatUrl + ": " + e.getFaultInfo().getType() + " "
						+ e.getMessage();
				logger.error(msg);
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					throw new IllegalStateException(msg);
				}
			}
		}
		return icatService;
	}

	public String getIcatUrl() {
		return icatUrl;
	}

	public String getJmsTopicConnectionFactory() {
		return jmsTopicConnectionFactory;
	}

	public String getKey() {
		return key;
	}

	public long getLinkLifetimeMillis() {
		return linkLifetimeMillis;
	}

	public Set<CallType> getLogSet() {
		return logSet;
	}

	public MainStorageInterface getMainStorage() {
		return mainStorage;
	}

	public synchronized int getMaxEntities() {
		// Keep trying every 10 seconds to connect to ICAT. Each failure
		// will produce an error message.
		while (maxEntities == null) {
			try {
				org.icatproject.icat.client.ICAT ricat = new org.icatproject.icat.client.ICAT(icatUrl);

				try (JsonReader parser = Json
						.createReader(new ByteArrayInputStream(ricat.getProperties().getBytes()))) {
					maxEntities = parser.readObject().getInt("maxEntities");
					logger.info("maxEntities from the ICAT.server {} version {} is {}", icatUrl, ricat.getVersion(),
							maxEntities);
				} catch (Exception e) {
					String msg = "Problem finding 1 ICAT API version " + e.getClass() + " " + e.getMessage();
					logger.error(msg);
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						throw new IllegalStateException(msg);
					}
				}
			} catch (URISyntaxException e) {
				String msg = "Problem finding 2 ICAT API version " + e.getClass() + " " + e.getMessage();
				logger.error(msg);
				throw new IllegalStateException(msg);
			}
		}
		return maxEntities;
	}

	public int getMaxIdsInQuery() {
		return maxIdsInQuery;
	}

	public int getPreparedCount() {
		return preparedCount;
	}

	public long getProcessQueueIntervalSeconds() {
		return processQueueIntervalSeconds;
	}

	public List<String> getReader() {
		return reader;
	}

	public boolean getReadOnly() {
		return readOnly;
	}

	public Set<String> getRootUserNames() {
		return rootUserNames;
	}

	public long getSizeCheckIntervalMillis() {
		return sizeCheckIntervalMillis;
	}

	public long getStartArchivingLevel() {
		return startArchivingLevel;
	}

	public long getStopArchivingLevel() {
		return stopArchivingLevel;
	}

	StorageUnit getStorageUnit() {
		return storageUnit;
	}

	public int getTidyBlockSize() {
		return tidyBlockSize;
	}

	public long getDelayDatasetWrites() {
		return delayDatasetWrites;
	}

	public long getDelayDatafileOperations() {
		return delayDatafileOperations;
	}

	public ZipMapperInterface getZipMapper() {
		return zipMapper;
	}

	public org.icatproject.icat.client.ICAT getRestIcat() {
		return restIcat;
	}

	public boolean getUseReaderForPerformance() {
		return useReaderForPerformance;
	}
}
