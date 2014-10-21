package org.icatproject.ids;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
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
	private boolean compressDatasetCache;
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
	private long writeDelaySeconds;
	private ZipMapperInterface zipMapper;

	@SuppressWarnings("unchecked")
	private PropertyHandler() {

		CheckedProperties props = new CheckedProperties();

		try {
			props.loadFromFile("ids.properties");
			logger.info("Property file ids.properties loaded");

			// do some very basic error checking on the config options
			String icatUrl = props.getProperty("icat.url");
			if (!icatUrl.endsWith("ICATService/ICAT?wsdl")) {
				if (icatUrl.charAt(icatUrl.length() - 1) == '/') {
					icatUrl = icatUrl + "ICATService/ICAT?wsdl";
				} else {
					icatUrl = icatUrl + "/ICATService/ICAT?wsdl";
				}
			}

			try {
				URL url = new URL(icatUrl);
				icatService = new ICATService(url, new QName("http://icatproject.org",
						"ICATService")).getICATPort();
			} catch (MalformedURLException e) {
				String msg = "Invalid property icat.url (" + icatUrl + "). Check URL format";
				logger.error(msg);
				throw new IllegalStateException(msg);
			}
			try {
				icatService.getApiVersion();// make a test call
			} catch (IcatException_Exception e) {
				String msg = "Problem finding ICAT API version " + e.getFaultInfo().getType() + " "
						+ e.getMessage();
				logger.error(msg);
				throw new IllegalStateException(msg);
			}

			preparedCount = props.getPositiveInt("preparedCount");
			processQueueIntervalSeconds = props.getPositiveLong("processQueueIntervalSeconds");
			rootUserNames = new HashSet<>(Arrays.asList(props.getString("rootUserNames").trim()
					.split("\\s+")));

			reader = Arrays.asList(props.getString("reader").trim().split("\\s+"));
			if (reader.size() % 2 != 1) {
				throw new IllegalStateException("reader must have an odd number of words");
			}

			readOnly = props.getBoolean("readOnly", false);

			sizeCheckIntervalMillis = props.getPositiveInt("sizeCheckIntervalSeconds") * 1000L;

			try {
				Class<ZipMapperInterface> klass = (Class<ZipMapperInterface>) Class.forName(props
						.getString("plugin.zipMapper.class"));
				zipMapper = klass.newInstance();
				logger.debug("ZipMapper initialised");
			} catch (Exception e) {
				abort(e.getClass() + " " + e.getMessage());
			}

			try {
				Class<MainStorageInterface> klass = (Class<MainStorageInterface>) Class
						.forName(props.getString("plugin.main.class"));
				mainStorage = klass.getConstructor(File.class).newInstance(
						props.getFile("plugin.main.properties"));
				logger.debug("mainStorage initialised");
			} catch (InvocationTargetException e) {
				Throwable cause = e.getCause();
				abort(cause.getClass() + " " + cause.getMessage());
			} catch (Exception e) {
				abort(e.getClass() + " " + e.getMessage());
			}

			if (props.getProperty("plugin.archive.class") == null) {
				logger.info("Property plugin.archive.class not set, single storage enabled.");
			} else {
				writeDelaySeconds = props.getPositiveLong("writeDelaySeconds");
				compressDatasetCache = props.getBoolean("compressDatasetCache", false);

				try {
					Class<ArchiveStorageInterface> klass = (Class<ArchiveStorageInterface>) Class
							.forName(props.getString("plugin.archive.class"));
					archiveStorage = klass.getConstructor(File.class).newInstance(
							props.getFile("plugin.archive.properties"));
					logger.debug("archiveStorage initialised");
				} catch (InvocationTargetException e) {
					Throwable cause = e.getCause();
					abort(cause.getClass() + " " + cause.getMessage());
				} catch (Exception e) {
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
					abort("storageUnit value " + props.getString("storageUnit")
							+ " must be taken from " + vs);
				}
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

	public ICAT getIcatService() {
		return icatService;
	}

	public long getlinkLifetimeMillis() {
		return linkLifetimeMillis;
	}

	public MainStorageInterface getMainStorage() {
		return mainStorage;
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

	public long getWriteDelaySeconds() {
		return writeDelaySeconds;
	}

	public ZipMapperInterface getZipMapper() {
		return zipMapper;
	}

	public boolean isCompressDatasetCache() {
		return compressDatasetCache;
	}

}
