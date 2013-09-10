/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.icatproject.ids.storage;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;

public interface StorageInterface {
	public void getDataset(Dataset dataset, OutputStream os) throws Exception;
	public InputStream getDatasetInputStream(Dataset dataset) throws Exception;
	public void putDataset(Dataset dataset, InputStream is) throws Exception;
	public boolean datasetExists(Dataset dataset) throws Exception;
	public void deleteDataset(Dataset dataset) throws Exception;
	
	/**
	 * Returns the length of the created file
	 */
	public long putDatafile(String location, InputStream is, Dataset dataset) throws Exception;
	
	public void prepareZipForRequest(Set<Dataset> datasets, Set<Datafile> datafiles, String zipName, boolean compress) throws Exception;
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws Exception;
}
