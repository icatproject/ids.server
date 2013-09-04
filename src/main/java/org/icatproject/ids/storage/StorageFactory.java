package org.icatproject.ids.storage;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.icatproject.ids.storage.local.LocalFileStorage;
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

    private StorageInterface createStorageInterface(Class<StorageInterface> storageInterfaceImplementation) {
    	logger.info("creatingStorageInterface");
    	StorageInterface ret = null;
    	try {
//    		Constructor<StorageInterface> constructor = 
//    				storageInterfaceImplementation.getConstructor(Map.class);
//	    	ret = constructor.newInstance(props);
    		Constructor<StorageInterface> constructor = storageInterfaceImplementation.getConstructor();
    		ret = constructor.newInstance();
    	} catch (Exception e) {
    		logger.error("Could not instantiate StorageInterface implementation " + LocalFileStorage.class.getCanonicalName());
    		throw new RuntimeException(e);
    	}
    	logger.info("created StorageInterface " + ret);
		return ret;
    }
    
    public StorageInterface createFastStorageInterface() {
    	return createStorageInterface(properties.getFastStorageInterfaceImplementation());
    }
    
    public StorageInterface createSlowStorageInterface() {
    	return createStorageInterface(properties.getSlowStorageInterfaceImplementation());
    }
}
