package org.icatproject.ids.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;

import org.icatproject.ids.storage.StorageInterface;
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
    private Class<StorageInterface> fastStorageInterfaceImplementation;
    private Class<StorageInterface> slowStorageInterfaceImplementation;

    @SuppressWarnings("unchecked")
	private PropertyHandler() {
        File f = new File("ids.properties");
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(f));
            logger.info("Property file " + f + " loaded");
        } catch (Exception e) {
            String msg = "Problem with " + f.getAbsolutePath() + ": " + e.getMessage();
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        // do some very basic error checking on the config options
        icatUrl = props.getProperty("icat.url");
        if (!icatUrl.endsWith("ICATService/ICAT?wsdl")) {
        	if (icatUrl.charAt(icatUrl.length()-1) == '/') {
        		icatUrl = icatUrl + "ICATService/ICAT?wsdl";
        	} else {
        		icatUrl = icatUrl + "/ICATService/ICAT?wsdl";
        	}
        }
        try {
            final URLConnection connection = new URL(icatUrl).openConnection();
            connection.connect();
        } catch (MalformedURLException e) {
            String msg = "Invalid property icat.url (" + icatUrl + "). Check URL format";
            logger.error(msg);
            throw new IllegalStateException(msg);
        } catch (IOException e) {
            String msg = "Unable to contact URL supplied for icat.url (" + icatUrl + ")";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        requestExpireTimeDays = Integer.parseInt(props.getProperty("requestExpireTimeDays"));
        if (requestExpireTimeDays < 1) {
            String msg = "Invalid property requestExpireTimeDays ("
                    + props.getProperty("requestExpireTimeDays")
                    + "). Must be an integer greater than 0.";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }
        
        writeDelaySeconds = Long.parseLong(props.getProperty("writeDelaySeconds"));
        if (writeDelaySeconds < 1) {
            String msg = "Invalid property writeDelaySeconds ("
                    + props.getProperty("writeDelaySeconds")
                    + "). Must be an integer greater than 0.";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }
        
        processQueueIntervalSeconds = Long.parseLong(props.getProperty("processQueueIntervalSeconds"));
        if (processQueueIntervalSeconds < 1) {
            String msg = "Invalid property processQueueIntervalSeconds ("
                    + props.getProperty("processQueueIntervalSeconds")
                    + "). Must be an integer greater than 0.";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }
        
        String fastStorageInterfaceImplementationName = props.getProperty("plugin.main");
        if (fastStorageInterfaceImplementationName == null) {
        	String msg = "Property plugin.main must be set.";
        	logger.error(msg);
        	throw new IllegalStateException(msg);
        }
        try {
        	fastStorageInterfaceImplementation = (Class<StorageInterface>) Class.forName(fastStorageInterfaceImplementationName);
        } catch (Exception e) {
        	String msg = "Could not get class implementing StorageInterface from " + fastStorageInterfaceImplementationName;
        	logger.error(msg);
        	throw new IllegalStateException(msg);
        }
        
        String slowStorageInterfaceImplementationName = props.getProperty("plugin.archive");
        if (slowStorageInterfaceImplementationName == null) {
        	String msg = "Property plugin.archive must be set.";
        	logger.error(msg);
        	throw new IllegalStateException(msg);
        }
        try {
        	slowStorageInterfaceImplementation = (Class<StorageInterface>) Class.forName(slowStorageInterfaceImplementationName);
        } catch (Exception e) {
        	String msg = "Could not get class implementing StorageInterface from " + slowStorageInterfaceImplementationName;
        	logger.error(msg);
        	throw new IllegalStateException(msg);
        }
    }

    public static PropertyHandler getInstance() {
        if (instance == null) {
            instance = new PropertyHandler();
        }
        return instance;
    }

    public String getIcatUrl() {
        return icatUrl;
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

	public Class<StorageInterface> getFastStorageInterfaceImplementation() {
		return fastStorageInterfaceImplementation;
	}
	
	public Class<StorageInterface> getSlowStorageInterfaceImplementation() {
		return slowStorageInterfaceImplementation;
	}
}
