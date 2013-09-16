package org.icatproject.ids.storage.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageInterface;

public class SlowLocalFileStorage implements StorageInterface {
	
	final String STORAGE_ARCHIVE_DIR = StoragePropertyHandler.getInstance().getStorageArchiveDir();
	final LocalFileStorageCommons fsCommons = new LocalFileStorageCommons();
	
	@Override
	public boolean datasetExists(Dataset dataset) throws IOException {
		return fsCommons.datasetExists(dataset, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public InputStream getDataset(Dataset dataset) throws IOException {
		return fsCommons.getDatasetInputStream(dataset, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public void putDataset(Dataset dataset, InputStream is) throws IOException {
		fsCommons.putDataset(dataset, is, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public void deleteDataset(Dataset dataset) throws IOException {
		fsCommons.deleteDataset(dataset, STORAGE_ARCHIVE_DIR);
	}
	
	@Override
	public long putDatafile(Datafile datafile, InputStream is) throws IOException {
		throw new UnsupportedOperationException("Single files cannot be added directly to the slow storage");
	}
	
	@Override
	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws IOException {
		throw new UnsupportedOperationException("This storage can't prepare zip files for users");
	}
	
	@Override
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws IOException {
		throw new UnsupportedOperationException("This storage can't prepare zip files for users");
	}

	@Override
	public InputStream getDatafile(Datafile datafile) throws IOException {
		throw new UnsupportedOperationException("This storage can't stream single datafiles");
	}

	@Override
	public long putDatafile(String relativeLocation, InputStream is) throws IOException {
		throw new UnsupportedOperationException("Single files cannot be added directly to the slow storage");
	}

}
