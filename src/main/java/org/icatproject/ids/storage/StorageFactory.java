package org.icatproject.ids.storage;

import java.lang.reflect.Constructor;

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

    public StorageInterface createStorageInterface() {
    	logger.info("creatingStorageInterface");
    	StorageInterface ret = null;
    	try {
    		Constructor<StorageInterface> constructor = 
    				properties.getStorageInterfaceImplementation().getConstructor(String.class, String.class, String.class);
	    	ret = constructor.newInstance(properties.getStorageDir(), properties.getStorageZipDir(), properties.getStorageArchiveDir());
    	} catch (Exception e) {
    		logger.error("Could not instantiate StorageInterface implementation " + LocalFileStorage.class.getCanonicalName());
    		throw new RuntimeException(e);
    	}
    	logger.info("created StorageInterface " + ret);
		return ret;
    }
}
