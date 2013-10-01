package org.icatproject.ids.storage;

import java.io.IOException;
import java.io.InputStream;

public interface StorageInterface {
	public boolean datafileExists(String location) throws IOException;

	public boolean datasetExists(String location) throws IOException;

	public void deleteDatafile(String location) throws IOException;

	public void deleteDataset(String location) throws IOException;

	public InputStream getDatafile(String location) throws IOException;

	public InputStream getDataset(String location) throws IOException;

	public InputStream getPreparedZip(String zipName, long offset) throws IOException;

	/** Write to datafile file at location and leave the input stream open */
	public long putDatafile(String location, InputStream is) throws IOException;

	/** Write to dataset file at location and close the input stream */
	public void putDataset(String location, InputStream is) throws IOException;

	/** Write to zip file at location and close the input stream */
	public void putPreparedZip(String zipName, InputStream is) throws IOException;
}
