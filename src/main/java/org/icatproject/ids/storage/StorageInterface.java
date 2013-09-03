/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.icatproject.ids.storage;

import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;

public interface StorageInterface {
    public StatusInfo restoreFromArchive(Dataset dataset);
    public StatusInfo writeToArchive(Dataset dataset);
    // not yet implemented
//    public StatusInfo copyToArchive(Ids2DatasetEntity dataset);
    
    // old methods, unused
//    public String getStoragePath();
//    public void clearUnusedFiles(int numberOfDays);
}
