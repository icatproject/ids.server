package org.icatproject.ids.storage;

import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.icatproject.ids.storage.local.LocalFileStorage;
import org.icatproject.ids.util.PropertyHandler;

public class StorageFactory {
	
	private final static Logger logger = Logger.getLogger(StorageFactory.class.getName());
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
    	logger.log(Level.INFO, "creatingStorageInterface");
    	StorageInterface ret = null;
    	try {
    		Constructor<StorageInterface> constructor = 
    				properties.getStorageInterfaceImplementation().getConstructor(String.class, String.class, String.class);
	    	ret = constructor.newInstance(properties.getStorageDir(), properties.getStorageZipDir(), properties.getStorageArchiveDir());
    	} catch (Exception e) {
    		logger.log(Level.SEVERE, "Could not instantiate StorageInterface implementation " + LocalFileStorage.class.getCanonicalName());
    		throw new RuntimeException(e);
    	}
    	logger.log(Level.INFO, "created StorageInterface " + ret);
		return ret;
    }
}
