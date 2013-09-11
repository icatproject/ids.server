package org.icatproject.ids.storage.local;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.icatproject.ids.util.PropertyHandler;
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
		File f = new File("ids_storage.properties");
		Properties props = new Properties();
		try {
			props.load(new FileInputStream(f));
			logger.info("Property file " + f + " loaded");
		} catch (Exception e) {
			String msg = "Problem with " + f.getAbsolutePath() + ": " + e.getMessage();
			logger.error(msg);
			throw new IllegalStateException(msg);
		}

		storageArchiveDir = setICATDirFromProperties(props, "STORAGE_ARCHIVE_DIR");
		storageZipDir = setICATDirFromProperties(props, "STORAGE_ZIP_DIR");
		storageDir = setICATDirFromProperties(props, "STORAGE_DIR");
		storagePreparedDir = setICATDirFromProperties(props, "STORAGE_PREPARED_DIR");
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
