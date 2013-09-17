package org.icatproject.ids.storage.local;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FastLocalFileStorage implements StorageInterface {
	
	@SuppressWarnings("unused")
	private final static Logger logger = LoggerFactory.getLogger(FastLocalFileStorage.class);
	
	final int BUFSIZ = 2048;
	final StoragePropertyHandler storagePropertyHandler = StoragePropertyHandler.getInstance();
	final String STORAGE_ZIP_DIR = storagePropertyHandler.getStorageZipDir();
	final String STORAGE_DIR = storagePropertyHandler.getStorageDir();
	final String STORAGE_PREPARED_DIR = storagePropertyHandler.getStoragePreparedDir();
	final LocalFileStorageCommons fsCommons = new LocalFileStorageCommons();
	
	@Override
	public InputStream getDataset(Dataset dataset) throws IOException {
		return fsCommons.getDatasetInputStream(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public void putDataset(Dataset dataset, InputStream is) throws IOException {
		fsCommons.putDataset(dataset, is, STORAGE_ZIP_DIR);
	}
	
	@Override
	public void deleteDataset(Dataset dataset) throws IOException {
		fsCommons.deleteDataset(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public boolean datasetExists(Dataset dataset) throws IOException {
		return fsCommons.datasetExists(dataset, STORAGE_ZIP_DIR);
	}
	
	@Override
	public InputStream getDatafile(Datafile datafile) throws FileNotFoundException {
		File file = new File(STORAGE_DIR, datafile.getLocation());
		if (!file.exists()) {
			throw new FileNotFoundException(file.getAbsolutePath());
		}
		return new BufferedInputStream(new FileInputStream(file));
	}
	
	@Override
	public long putDatafile(Datafile datafile, InputStream is) throws IOException {
		File file = new File(new File(STORAGE_DIR, datafile.getDataset().getLocation()), datafile.getName());
		fsCommons.writeInputStreamToFile(file, is);
		return file.length();
	};
	
	@Override
	public void deleteDatafile(Datafile datafile) throws IOException {
		File file = new File(STORAGE_DIR, datafile.getLocation());
		file.delete();
	}
	
	@Override
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws IOException {
		fsCommons.getPreparedZip(zipName, os, offset, STORAGE_PREPARED_DIR);
	}

	@Override
	public long putDatafile(String relativeLocation, InputStream is) throws IOException {
		File file = new File(STORAGE_DIR, relativeLocation);
		fsCommons.writeInputStreamToFile(file, is);
		return file.length();
	}

	@Override
	public void putPreparedZip(String zipName, InputStream is) throws IOException {
		File file = new File(STORAGE_PREPARED_DIR, zipName);
		fsCommons.writeInputStreamToFile(file, is);
	}

}
