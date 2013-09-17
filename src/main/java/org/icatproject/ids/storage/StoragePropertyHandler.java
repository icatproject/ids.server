package org.icatproject.ids.storage;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePropertyHandler {

	private static final Logger logger = LoggerFactory.getLogger(StoragePropertyHandler.class);
	private static StoragePropertyHandler instance = null;

	private String fastStorageZipDir;
	private String fastStorageDir;
	private String fastStoragePreparedDir;
	private String slowStorageZipDir;
	private String slowStorageDir;
	private String slowStoragePreparedDir;

	private StoragePropertyHandler() {
		File f = new File("ids-storage.properties");
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(f));
			logger.info("Property file " + f + " loaded");
		} catch (Exception e) {
			String msg = "Problem with " + f.getAbsolutePath() + ": " + e.getMessage();
			logger.error(msg);
			throw new IllegalStateException(msg);
		}
		
		fastStorageZipDir = setICATDirFromProperties(props, "fast.storageZipDir");
		fastStorageDir = setICATDirFromProperties(props, "fast.storageDir");
		fastStoragePreparedDir = setICATDirFromProperties(props, "fast.storagePreparedDir");

		slowStorageZipDir = setICATDirFromProperties(props, "slow.storageZipDir");
		slowStorageDir = setICATDirFromProperties(props, "slow.storageDir");
		slowStoragePreparedDir = setICATDirFromProperties(props, "slow.storagePreparedDir");
	}
	
	public static StoragePropertyHandler getInstance() {
        if (instance == null) {
            instance = new StoragePropertyHandler();
        }
        return instance;
    }	

	public String getFastStorageZipDir() {
		return fastStorageZipDir;
	}

	public String getFastStorageDir() {
		return fastStorageDir;
	}

	public String getFastStoragePreparedDir() {
		return fastStoragePreparedDir;
	}

	public String getSlowStorageZipDir() {
		return slowStorageZipDir;
	}

	public String getSlowStorageDir() {
		return slowStorageDir;
	}

	public String getSlowStoragePreparedDir() {
		return slowStoragePreparedDir;
	}

	private String setICATDirFromProperties(Properties props, String property) {
		String res = props.getProperty(property);
		if (res == null) {
			String msg = "Property " + property + " left unset.";
			logger.info(msg);
			return null;
		}

		File tmp = new File(res);
		if (!tmp.exists()) {
			String msg = "Invalid " + property + ". Directory " + res + " not found. Please create.";
			logger.error(msg);
			throw new IllegalStateException(msg);
		}
		return res;
	}

}
