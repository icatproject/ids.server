package org.icatproject.ids.storage.local;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.icatproject.Datafile;
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
	
	@Override
	public void deleteDataset(Dataset dataset) throws Exception {
		fsCommons.deleteDataset(dataset, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public void prepareZipForRequest(Set<Datafile> datafiles, String zipName, boolean compress) throws Exception {
		throw new UnsupportedOperationException("This storage can't prepare zip files for users");
	}
	
	@Override
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws Exception {
		throw new UnsupportedOperationException("This storage can't prepare zip files for users");
	}

}
