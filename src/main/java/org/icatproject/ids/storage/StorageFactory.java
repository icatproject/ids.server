package org.icatproject.ids.storage;

import java.lang.reflect.Constructor;

import org.icatproject.ids.util.PropertyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageFactory {
	
	private final static Logger logger = LoggerFactory.getLogger(StorageFactory.class);
	private PropertyHandler properties = PropertyHandler.getInstance();

    private static StorageFactory instance = null;

    private StorageFactory() {}

    public static StorageFactory getInstance() {
        if (instance == null) {
            instance = new StorageFactory();
        }
        return instance;
    }

    private StorageInterface createStorageInterface(Class<StorageInterface> storageInterfaceImplementation, StorageType storageType) {
    	logger.info("creatingStorageInterface " + storageInterfaceImplementation.getCanonicalName());
    	StorageInterface ret = null;
    	try {
    		Constructor<StorageInterface> constructor = storageInterfaceImplementation.getConstructor(StorageType.class);
    		ret = constructor.newInstance(storageType);
    	} catch (Exception e) {
    		logger.error("Could not instantiate StorageInterface implementation " + storageInterfaceImplementation.getCanonicalName());
    		throw new RuntimeException(e);
    	}
    	logger.info("created StorageInterface " + ret);
		return ret;
    }
    
    public StorageInterface createFastStorageInterface() {
    	return createStorageInterface(properties.getFastStorageInterfaceImplementation(), StorageType.FAST);
    }
    
    public StorageInterface createSlowStorageInterface() {
    	return createStorageInterface(properties.getSlowStorageInterfaceImplementation(), StorageType.SLOW);
    }
}
