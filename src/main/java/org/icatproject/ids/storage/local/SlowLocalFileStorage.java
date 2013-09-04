package org.icatproject.ids.storage.local;

import java.io.InputStream;
import java.io.OutputStream;

import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;

public class SlowLocalFileStorage implements StorageInterface {
	
	final String STORAGE_ARCHIVE_DIR = "/home/wojtek/icat/icatarchive/";
	final LocalFileStorageCommons fsCommons = new LocalFileStorageCommons();
	
	@Override
	public boolean datasetExists(Dataset dataset) throws Exception {
		return fsCommons.datasetExists(dataset, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public void getDataset(Dataset dataset, OutputStream os) throws Exception {
		fsCommons.getDataset(dataset, os, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public InputStream getDatasetInputStream(Dataset dataset) throws Exception {
		return fsCommons.getDatasetInputStream(dataset, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public void putDataset(Dataset dataset, InputStream is) throws Exception {
		fsCommons.putDataset(dataset, is, STORAGE_ARCHIVE_DIR);
	}

}
