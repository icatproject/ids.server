package org.icatproject.ids.storage;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.persistence.EntityManager;

import org.icatproject.ids.storage.local.LocalFileStorage;
import org.icatproject.ids.util.PropertyHandler;

public class StorageFactory {
	
	private final static Logger logger = Logger.getLogger(StorageFactory.class.getName());
	private PropertyHandler properties = PropertyHandler.getInstance();

    private static StorageFactory instance = null;

    private StorageFactory() {}

    public StorageInterface createStorageInterface(EntityManager em, String requestid) {
    	logger.log(Level.INFO, "createStorageInterface");
//    	return new LocalFileStorage();
    	StorageInterface ret = null;
    	try {
//	    	String className = "org.icatproject.ids.storage.local.LocalFileStorage";
//	    	ClassLoader classLoader = ClassLoader.getSystemClassLoader();
	    	
//	    	Class<StorageInterface> clazz = (Class<StorageInterface>) classLoader.loadClass(LocalFileStorage.class.getCanonicalName());
//	    	Class<StorageInterface> clazz = (Class<StorageInterface>) Class.forName(className);
//	    	logger.log(Level.INFO, "clazz = " + clazz);
	    	ret = properties.getStorageInterfaceImplementation().newInstance();
    	} catch (Exception e) {
    		logger.log(Level.SEVERE, "Could not instantiate StorageInterface implementation " + LocalFileStorage.class.getCanonicalName());
//    		logger.log(Level.INFO, "current classpath: " + System.getProperty("java.classpath"));
    		throw new RuntimeException(e);
    	}
    	logger.log(Level.INFO, "created StorageInterface " + ret);
		return ret;
    }

    public static StorageFactory getInstance() {
        if (instance == null) {
            instance = new StorageFactory();
        }
        return instance;
    }
}
