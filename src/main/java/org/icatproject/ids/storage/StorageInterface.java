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
	
	/*
	 * Returns the length of the created file
	 */
	public long putDatafile(String location, InputStream is) throws Exception;
//	public boolean datafileExists(Datafile datafile) throws Exception;
	
	public void prepareZipForRequest(Set<Datafile> datafiles, String zipName, boolean compress) throws Exception;
	public void getPreparedZip(String zipName, OutputStream os, long offset) throws Exception;
	
//    public StatusInfo restoreFromArchive(Dataset dataset);
//    public StatusInfo writeToArchive(Dataset dataset);
	
    // not yet implemented
//    public StatusInfo copyToArchive(Ids2DatasetEntity dataset);
    
    // old methods, unused
//    public String getStoragePath();
//    public void clearUnusedFiles(int numberOfDays);
}
