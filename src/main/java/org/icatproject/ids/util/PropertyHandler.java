package org.icatproject.ids.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;

import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.plugin.StorageInterface;
import org.icatproject.ids.plugin.StorageType;
import org.icatproject.utils.CheckedProperties;
import org.icatproject.utils.CheckedProperties.CheckedPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Load the properties specified in the properties file ids.properties.
 */
public class PropertyHandler {

	private static final Logger logger = LoggerFactory.getLogger(PropertyHandler.class);
	private static PropertyHandler instance = null;

	private int requestExpireTimeDays;
	private String icatUrl;
	private long writeDelaySeconds;
	private long processQueueIntervalSeconds;
	private StorageInterface mainStorage;
	private StorageInterface archiveStorage;
	private File tmpDir;
	private ICAT icatService;

	@SuppressWarnings("unchecked")
	private PropertyHandler() {

		CheckedProperties props = new CheckedProperties();

		try {
			props.loadFromFile("ids.properties");
			logger.info("Property file ids.properties loaded");

			// do some very basic error checking on the config options
			icatUrl = props.getProperty("icat.url");
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

			requestExpireTimeDays = props.getPositiveInt("requestExpireTimeDays");
			writeDelaySeconds = props.getPositiveLong("writeDelaySeconds");
			processQueueIntervalSeconds = props.getPositiveLong("processQueueIntervalSeconds");

			try {
				Class<StorageInterface> klass = (Class<StorageInterface>) Class.forName(props
						.getString("plugin.main.class"));
				mainStorage = klass.getConstructor(File.class, StorageType.class).newInstance(
						props.getFile("plugin.main.properties"), StorageType.MAIN);
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
				try {
					Class<StorageInterface> klass = (Class<StorageInterface>) Class.forName(props
							.getString("plugin.archive.class"));
					archiveStorage = klass.getConstructor(File.class, StorageType.class)
							.newInstance(props.getFile("plugin.archive.properties"),
									StorageType.ARCHIVE);
					logger.debug("archiveStorage initialised");
				} catch (InvocationTargetException e) {
					Throwable cause = e.getCause();
					abort(cause.getClass() + " " + cause.getMessage());
				} catch (Exception e) {
					abort(e.getClass() + " " + e.getMessage());
				}
			}

			tmpDir = props.getFile("tmpDir");
			if (!tmpDir.isDirectory()) {
				abort(tmpDir + " must be an existing directory");
			}

		} catch (CheckedPropertyException e) {
			abort(e.getMessage());
		}
	}

	private void abort(String msg) {
		logger.error(msg);
		logger.error("IllegalStateException being thrown");
		throw new IllegalStateException(msg);
	}

	public synchronized static PropertyHandler getInstance() {
		if (instance == null) {
			instance = new PropertyHandler();
		}
		return instance;
	}

	public long getWriteDelaySeconds() {
		return writeDelaySeconds;
	}

	public long getProcessQueueIntervalSeconds() {
		return processQueueIntervalSeconds;
	}

	public int getRequestExpireTimeDays() {
		return requestExpireTimeDays;
	}

	public StorageInterface getMainStorage() {
		return mainStorage;
	}

	public StorageInterface getArchiveStorage() {
		return archiveStorage;
	}

	public File getTmpDir() {
		return tmpDir;
	}

	public ICAT getIcatService() {
		return icatService;
	}
}
