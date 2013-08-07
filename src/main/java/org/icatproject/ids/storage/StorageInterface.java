/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.icatproject.ids.storage;

import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.Ids2DatasetEntity;

public interface StorageInterface {
    public StatusInfo restoreFromArchive(Ids2DatasetEntity dataset);
    // not yet implemented
//    public StatusInfo copyToArchive(Ids2DatasetEntity dataset);
    
    // old methods, unused
//    public String getStoragePath();
//    public void clearUnusedFiles(int numberOfDays);
}
