package org.icatproject.ids;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonReader;

import org.icatproject.ICAT;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.ids.storage.ArchiveStorageInterfaceDLS;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to load the properties specified in the main configuration file 
 * run.properties
 */
public class PropertyHandler {

	private static final Logger logger = LoggerFactory.getLogger(PropertyHandler.class);

	private static PropertyHandler instance = null;

	private Properties simpleProps;

	// ICAT related fields
	private String icatUrl;
	private ICAT icatService;
	private org.icatproject.icat.client.ICAT restIcat;
	private Integer maxEntities;
	private Set<String> rootUserNames;
	private List<String> reader;
	private boolean useReaderForPerformance;

	// Storage/plugin related fields
	private MainStorageInterface mainStorage;
	private Class<ArchiveStorageInterfaceDLS> archiveStorageClass;
	private ZipMapperInterface zipMapper;
	private String missingFilesZipEntryName;
	private int maxRestoresPerThread;

	private Path cacheDir;

	// number of files to hold in the cache sub-directories
	private int preparedCount;
	private int completedCount;
	private int failedFilesCount;

	// properties for the Tidier
	private long sizeCheckIntervalMillis;
	private long startArchivingLevel;
	private long stopArchivingLevel;


	public static synchronized PropertyHandler getInstance() {
		if (instance == null) {
			instance = new PropertyHandler();
		}
		return instance;
	}

	@SuppressWarnings("unchecked")
	private PropertyHandler() {

		CheckedProperties props = new CheckedProperties();

		try {
			props.loadFromResource(Constants.RUN_PROPERTIES_FILENAME);
			logger.info("Property file run.properties loaded");

			icatUrl = ICATGetter.getCleanUrl(props.getString("icat.url"));
			try {
				restIcat = new org.icatproject.icat.client.ICAT(icatUrl);
			} catch (URISyntaxException e) {
				abort(e.getMessage());
			}

			preparedCount = props.getPositiveInt("preparedCount");
			completedCount = props.getPositiveInt("completedCount");
			failedFilesCount = props.getPositiveInt("failedFilesCount");
			rootUserNames = new HashSet<>(Arrays.asList(props.getString("rootUserNames").trim().split("\\s+")));

			reader = Arrays.asList(props.getString("reader").trim().split("\\s+"));
			if (reader.size() % 2 != 1) {
				throw new IllegalStateException("reader must have an odd number of words");
			}

			sizeCheckIntervalMillis = props.getPositiveInt("sizeCheckIntervalSeconds") * 1000L;

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
			simpleProps = new Properties();
			try (InputStream is = getClass().getClassLoader().getResourceAsStream(Constants.RUN_PROPERTIES_FILENAME)) {
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

			// ensure plugin.archive.class is set and it is possible to initialise an instance of it
			// note that the test instance created here is no longer used once the check is complete
			if (!props.has("plugin.archive.class")) {
				String message = "Property plugin.archive.class must be set in run.properties";
				logger.error(message);
				abort(message);
			} else {
				try {
					archiveStorageClass = (Class<ArchiveStorageInterfaceDLS>)Class.forName(props.getString("plugin.archive.class"));
					ArchiveStorageInterfaceDLS archiveStorage = archiveStorageClass.getConstructor(Properties.class).newInstance(simpleProps);
					logger.debug("Test instance of ArchiveStorageInterfaceDLS successfully initialised");
				} catch (ClassNotFoundException | InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException | NoSuchMethodException
						| SecurityException | ClassCastException e) {
					logger.error("Failed to create test instance of ArchiveStorageInterfaceDLS", e);
					abort(e.getClass() + " " + e.getMessage());
				}
				startArchivingLevel = props.getPositiveLong("startArchivingLevel1024bytes") * 1024;
				stopArchivingLevel = props.getPositiveLong("stopArchivingLevel1024bytes") * 1024;
				if (stopArchivingLevel >= startArchivingLevel) {
					abort("startArchivingLevel1024bytes must be greater than stopArchivingLevel1024bytes");
				}

				maxRestoresPerThread = props.getPositiveInt("maxRestoresPerThread");
			}

			try {
				cacheDir = props.getFile("cache.dir").getCanonicalFile().toPath();
			} catch (IOException e) {
				abort("IOException " + e.getMessage());
			}
			if (!Files.isDirectory(cacheDir)) {
				abort(cacheDir + " must be an existing directory");
			}

			useReaderForPerformance = props.getBoolean("useReaderForPerformance", false);

			missingFilesZipEntryName = props.getString("missingFilesZipEntryName", 
					Constants.DEFAULT_MISSING_FILES_FILENAME);

		} catch (CheckedPropertyException e) {
			abort(e.getMessage());
		}
	}

	private void abort(String msg) {
		logger.error(msg);
		logger.error("IllegalStateException being thrown");
		throw new IllegalStateException(msg);
	}


	public Properties getSimpleProperties() {
		return simpleProps;
	}


	public MainStorageInterface getMainStorage() {
		return mainStorage;
	}

	public Class<ArchiveStorageInterfaceDLS> getArchiveStorageClass() {
		return archiveStorageClass;
	}

	public ZipMapperInterface getZipMapper() {
		return zipMapper;
	}

	public String getMissingFilesZipEntryName() {
		return missingFilesZipEntryName;
	}

    public int getMaxRestoresPerThread() {
        return maxRestoresPerThread;
    }


	public String getIcatUrl() {
		return icatUrl;
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

	public org.icatproject.icat.client.ICAT getRestIcat() {
		return restIcat;
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
					logger.info("maxEntities from the ICAT.server {} version {} is {}", 
							new Object[] {icatUrl, ricat.getVersion(), maxEntities});
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

	public List<String> getReader() {
		return reader;
	}

	public Set<String> getRootUserNames() {
		return rootUserNames;
	}

	public boolean getUseReaderForPerformance() {
		return useReaderForPerformance;
	}


	public Path getCacheDir() {
		return cacheDir;
	}

	public int getPreparedCount() {
		return preparedCount;
	}

	public int getCompletedCount() {
		return completedCount;
	}

	public int getFailedFilesCount() {
		return failedFilesCount;
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

}
