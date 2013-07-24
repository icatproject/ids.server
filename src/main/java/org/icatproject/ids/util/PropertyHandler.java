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
    private String localTemporaryStoragePath;
    private String storageType;
    private String localStorageSystemPath;
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

        localTemporaryStoragePath = props.getProperty("TEMPORARY_STORAGE_PATH");
        if (localTemporaryStoragePath == null) {
            String msg = "Property TEMPORARY_STORAGE_PATH must be set.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        File tmpStoragePathDir = new File(localTemporaryStoragePath);
        if (!tmpStoragePathDir.exists()) {
            String msg = "Invalid TEMPORARY_STORAGE_PATH. Directory " + localTemporaryStoragePath
                    + " not found. Please create.";
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

        storageType = props.getProperty("STORAGE_TYPE").trim().toUpperCase();
        if (storageType == null) {
            String msg = "Property STORAGE_TYPE must be set.";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        if (!"LOCAL".equals(storageType) && !"STORAGED".equals(storageType)) {
            String msg = "Invalid property STORAGE_TYPE (" + storageType
                    + "). Must be either LOCAL or STORAGED";
            logger.severe(msg);
            throw new IllegalStateException(msg);
        }

        if ("LOCAL".equals(storageType)) {
            localStorageSystemPath = props.getProperty("LOCAL_STORAGE_PATH").trim();
            File localStorageDir = new File(localStorageSystemPath);
            if (!localStorageDir.exists()) {
                String msg = "Invalid LOCAL_STORAGE_PATH. Directory " + localStorageSystemPath
                        + " not found. Please create.";
                logger.severe(msg);
                throw new IllegalStateException(msg);
            }
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

    public String getLocalStorageSystemPath() {
        return localStorageSystemPath;
    }

    public String getLocalTemporaryStoragePath() {
        return localTemporaryStoragePath;
    }

    public int getNumberOfDaysToExpire() {
        return numberOfDaysToExpire;
    }

    public String getStorageType() {
        return storageType;
    }

    public int getNumberOfDaysToKeepFilesInCache() {
        return numberOfDaysToKeepFilesInCache;
    }

	public Class<StorageInterface> getStorageInterfaceImplementation() {
		return storageInterfaceImplementation;
	}
}
