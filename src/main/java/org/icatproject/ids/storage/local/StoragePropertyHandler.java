package org.icatproject.ids.storage.local;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StoragePropertyHandler {

	private static final Logger logger = LoggerFactory.getLogger(StoragePropertyHandler.class);
	private static StoragePropertyHandler instance = null;

	private String storageArchiveDir;
	private String storageZipDir;
	private String storageDir;
	private String storagePreparedDir;

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

		storageArchiveDir = setICATDirFromProperties(props, "storageArchiveDir");
		storageZipDir = setICATDirFromProperties(props, "storageZipDir");
		storageDir = setICATDirFromProperties(props, "storageDir");
		storagePreparedDir = setICATDirFromProperties(props, "storagePreparedDir");
	}
	
	public static StoragePropertyHandler getInstance() {
        if (instance == null) {
            instance = new StoragePropertyHandler();
        }
        return instance;
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

	public String getStoragePreparedDir() {
		return storagePreparedDir;
	}

	private String setICATDirFromProperties(Properties props, String property) {
		String res = props.getProperty(property);
		if (res == null) {
			String msg = "Property " + property + " must be set.";
			logger.error(msg);
			throw new IllegalStateException(msg);
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
