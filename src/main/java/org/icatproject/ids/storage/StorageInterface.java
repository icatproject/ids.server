/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.icatproject.ids.storage;

import java.io.InputStream;
import java.io.OutputStream;

import org.icatproject.Dataset;

public interface StorageInterface {
	public void getDataset(Dataset dataset, OutputStream os) throws Exception;
	public InputStream getDatasetInputStream(Dataset dataset) throws Exception;
	public void putDataset(Dataset dataset, InputStream is) throws Exception;
	public boolean datasetExists(Dataset dataset) throws Exception;
	
//    public StatusInfo restoreFromArchive(Dataset dataset);
//    public StatusInfo writeToArchive(Dataset dataset);
	
    // not yet implemented
//    public StatusInfo copyToArchive(Ids2DatasetEntity dataset);
    
    // old methods, unused
//    public String getStoragePath();
//    public void clearUnusedFiles(int numberOfDays);
}
