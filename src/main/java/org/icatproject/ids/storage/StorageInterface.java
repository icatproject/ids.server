/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.icatproject.ids.storage;

import java.util.List;

import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;

public interface StorageInterface {
    public StatusInfo restoreFromArchive(List<Dataset> datasets);
    // not yet implemented
//    public StatusInfo copyToArchive(Ids2DatasetEntity dataset);
    
    // old methods, unused
//    public String getStoragePath();
//    public void clearUnusedFiles(int numberOfDays);
}
