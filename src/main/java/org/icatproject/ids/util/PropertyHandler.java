package org.icatproject.ids.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.logging.Logger;

import org.icatproject.ids.storage.StorageInterface;

/*
 * Load the properties specified in the properties file ids.properties.
 */
public class PropertyHandler {

    private static final Logger logger = Logger.getLogger(PropertyHandler.class.getName());
    private static PropertyHandler instance = null;

    private int numberOfDaysToExpire;
    private String icatURL;
    private String storageArchiveDir;
    private String storageZipDir;
    private String storageDir;
    private long writeDelaySeconds;
    private long processQueueIntervalSeconds;
    private int numberOfDaysToKeepFilesInCache;
    private Class<StorageInterface> storageInterfaceImplementation;

    @SuppressWarnings("unchecked")
	private PropertyHandler() {
        File f = new File("ids.properties");
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(f));
            logger.info("Property file " + f + " loaded");
        } catch (Exception e) {
            String msg = "Problem with " + f.getAbsolutePath() + ": " + e.getMessage();
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        // do some very basic error checking on the config options
        icatURL = props.getProperty("ICAT_URL");
        try {
            final URLConnection connection = new URL(icatURL).openConnection();
            connection.connect();
        } catch (MalformedURLException e) {
            String msg = "Invalid property ICAT_URL (" + icatURL + "). Check URL format";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        } catch (IOException e) {
            String msg = "Unable to contact URL supplied for ICAT_URL (" + icatURL + ")";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        numberOfDaysToExpire = Integer.parseInt(props.getProperty("NUMBER_OF_DAYS_TO_EXPIRE"));
        if (numberOfDaysToExpire < 1) {
            String msg = "Invalid property NUMBER_OF_DAYS_TO_EXPIRE ("
                    + props.getProperty("NUMBER_OF_DAYS_TO_EXPIRE")
                    + "). Must be an integer greater than 0.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        numberOfDaysToKeepFilesInCache = Integer.parseInt(props
                .getProperty("NUMBER_OF_DAYS_TO_KEEP_FILES_IN_CACHE"));
        if (numberOfDaysToKeepFilesInCache < 1) {
            String msg = "Invalid property NUMBER_OF_DAYS_TO_KEEP_FILES_IN_CACHE ("
                    + props.getProperty("NUMBER_OF_DAYS_TO_KEEP_FILES_IN_CACHE")
                    + ") Must be an integer greater than 0.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }
        
        storageArchiveDir = setICATDirFromProperties(props, "STORAGE_ARCHIVE_DIR");
        storageZipDir = setICATDirFromProperties(props, "STORAGE_ZIP_DIR");
        storageDir = setICATDirFromProperties(props, "STORAGE_DIR");
        
        writeDelaySeconds = Long.parseLong(props.getProperty("WRITE_DELAY_SECONDS"));
        if (writeDelaySeconds < 1) {
            String msg = "Invalid property WRITE_DELAY_SECONDS ("
                    + props.getProperty("WRITE_DELAY_SECONDS")
                    + "). Must be an integer greater than 0.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }
        
        processQueueIntervalSeconds = Long.parseLong(props.getProperty("PROCESS_QUEUE_INTERVAL_SECONDS"));
        if (processQueueIntervalSeconds < 1) {
            String msg = "Invalid property PROCESS_QUEUE_INTERVAL_SECONDS ("
                    + props.getProperty("PROCESS_QUEUE_INTERVAL_SECONDS")
                    + "). Must be an integer greater than 0.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }
        
        String storageInterfaceImplementationName = props.getProperty("STORAGE_INTERFACE_IMPLEMENTATION");
        if (storageInterfaceImplementationName == null) {
        	String msg = "Property STORAGE_INTERFACE_IMPLEMENTATION must be set.";
        	logger.severe(msg);
        	throw new IllegalStateException(msg);
        }
        try {
        	storageInterfaceImplementation = (Class<StorageInterface>) Class.forName(storageInterfaceImplementationName);
        } catch (Exception e) {
        	String msg = "Could not get class implementing StorageInterface from " + storageInterfaceImplementationName;
        	logger.severe(msg);
        	throw new IllegalStateException(msg);
        }
    }

    public static PropertyHandler getInstance() {
        if (instance == null) {
            instance = new PropertyHandler();
        }
        return instance;
    }

    public String getIcatURL() {
        return icatURL;
    }
    
    public String getStorageArchiveDir() {
    	return storageArchiveDir;
    }

    public String getStorageDir() {
        return storageDir;
    }

    public String getStorageZipDir() {
        return storageZipDir;
    }
    
    public long getWriteDelaySeconds() {
    	return writeDelaySeconds;
    }
    
    public long getProcessQueueIntervalSeconds() {
    	return processQueueIntervalSeconds;
    }

    public int getNumberOfDaysToExpire() {
        return numberOfDaysToExpire;
    }

    public int getNumberOfDaysToKeepFilesInCache() {
        return numberOfDaysToKeepFilesInCache;
    }

	public Class<StorageInterface> getStorageInterfaceImplementation() {
		return storageInterfaceImplementation;
	}
	
	private String setICATDirFromProperties(Properties props, String property) {
		String res = props.getProperty(property);
		// logger.severe(property + " = " + res); // TODO remove
        if (res == null) {
            String msg = "Property " + property + " must be set.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        File tmp = new File(res);
        if (!tmp.exists()) {
            String msg = "Invalid " + property + ". Directory " + res
                    + " not found. Please create.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }
        return res;
	}
}
