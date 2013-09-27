package org.icatproject.ids.storage;

import java.io.IOException;
import java.io.InputStream;
import org.icatproject.Datafile;
import org.icatproject.Dataset;

public interface StorageInterface {
	public InputStream getDataset(Dataset dataset) throws IOException;	
	public void putDataset(Dataset dataset, InputStream is) throws IOException;
	public void deleteDataset(Dataset dataset) throws IOException;	
	public void deleteDataset(String location) throws IOException;
	public boolean datasetExists(Dataset dataset) throws IOException;
	public boolean datasetExists(String location) throws IOException;
	
	public InputStream getDatafile(Datafile datafile) throws IOException;
	/*
     * putDatafile methods (both of them) don't close the InputStream!
     */
	public long putDatafile(Datafile datafile, InputStream is) throws IOException;
	public long putDatafile(String relativeLocation, InputStream is) throws IOException;
	public void deleteDatafile(Datafile datafile) throws IOException;
	public boolean datafileExists(Datafile datafile) throws IOException;
	
	public InputStream getPreparedZip(String zipName, long offset) throws IOException;
	public void putPreparedZip(String zipName, InputStream is) throws IOException;
}
