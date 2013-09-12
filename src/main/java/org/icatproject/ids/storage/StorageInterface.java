package org.icatproject.ids.storage;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;

public interface StorageInterface {
	/*
	 * Starts streaming data from the Dataset through the passed OutputStream
	 */
	public void getDataset(Dataset dataset, OutputStream os) throws Exception;
	
	/*
	 * Returns InputStream for the Dataset that the client can later write to
	 */
	public InputStream getDatasetInputStream(Dataset dataset) throws Exception;
	
	/*
	 * Creates a Dataset file on the storage and fills it with data from the InputStream
	 */
	public void putDataset(Dataset dataset, InputStream is) throws Exception;
	
	public boolean datasetExists(Dataset dataset) throws Exception;
	public void deleteDataset(Dataset dataset) throws Exception;
	
	/*
	 * Creates a new Datafile and fills it with data from the InputStream.
	 * The Datafile location is derived from its name and the Dataset's location.
	 * It is not possible to pass a Datafile object (instead of its name and its Dataset),
	 * because before invoking this method it's not actually a member of this Dataset.
	 * This method returns the size of the Datafile (in bytes) which is essential for
	 * creating a proper Datafile and then connecting it to the Dataset.
	 */
	public long putDatafile(String name, InputStream is, Dataset dataset) throws Exception;
	
	/*
	 * Prepares a zip file for the user to download. The hierarchy of the files within
	 * the zip is described in the interface specification. 
	 * zipName is the name of the zip that is to be created. It will be relative
	 * to the path specified in storagePreparedDir property in ids-storage.properties
	 */
	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws Exception;
	
	/*
	 * Starts streaming the data from the prepared zip through the passed OutputStream.
	 * The starting byte of the streamed data is specified by offset.
	 * The zipName is the name of the prepared zip file (essentially the same as in
	 * prepareZipForRequest method). A zip file with this name will be looked for
	 * under the path specified by storagePreparedDir property in ids-storage.properties
	 */
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws Exception;
}
