package org.icatproject.ids.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.icatproject.Datafile;
import org.icatproject.Dataset;

public interface StorageInterface {
	/*
	 * Returns InputStream for the Dataset that the client can later write to
	 */
	public InputStream getDataset(Dataset dataset) throws IOException;	
	/*
	 * Creates a Dataset file on the storage and fills it with data from the InputStream
	 */
	public void putDataset(Dataset dataset, InputStream is) throws IOException;
	public void deleteDataset(Dataset dataset) throws IOException;	
	public boolean datasetExists(Dataset dataset) throws IOException;
	
	public InputStream getDatafile(Datafile datafile) throws IOException;
	/*
	 * putDatafile methods (both of them) don't close the InputStream!
	 */
	public long putDatafile(Datafile datafile, InputStream is) throws IOException;
	public long putDatafile(String relativeLocation, InputStream is) throws IOException;
	public void deleteDatafile(Datafile datafile) throws IOException;
	
	/*
	 * Prepares a zip file for the user to download. The hierarchy of the files within
	 * the zip is described in the interface specification. 
	 * zipName is the name of the zip that is to be created. It will be relative
	 * to the path specified in storagePreparedDir property in ids-storage.properties
	 */
//	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws IOException;
	
	/*
	 * Starts streaming the data from the prepared zip through the passed OutputStream.
	 * The starting byte of the streamed data is specified by offset.
	 * The zipName is the name of the prepared zip file (essentially the same as in
	 * prepareZipForRequest method). A zip file with this name will be looked for
	 * under the path specified by storagePreparedDir property in ids-storage.properties
	 */
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws IOException;
	public void putPreparedZip(String zipName, InputStream is) throws IOException;
}
