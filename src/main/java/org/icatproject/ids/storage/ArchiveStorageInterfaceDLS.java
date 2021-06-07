package org.icatproject.ids.storage;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;

/**
 * A modified version of the ArchiveStorageInterface defined in the original
 * IDS plugin that also allows for monitoring the progress of restore requests.
 */
public interface ArchiveStorageInterfaceDLS {
    
    /**
     * Original method from the IDS plugin interface ArchiveStorageInterface
     * but with the added ability to throw an IOException 
     * 
     * @param mainStorageInterface MainStorageInterface as defined in the 
     *                             original IDS storage plugin
     * @param dfInfos A list of DatafileInfo objects containing the details of
     *                the datafiles to restore from archive storage (tape) to 
     *                main storage (the IDS disk cache)
     * @param stopRestoring a boolean flag that can be set in the parent
     *                      RestoreThread to indicate that the restore should 
     *                      be stopped (cleanly) at the next opportunity
     * @return A set of DatafileInfo objects for any files that failed to 
     *         restore
     * @throws IOException if there is a problem either reading files from 
     *                     StorageD or writing them to main storage
     */
    Set<DfInfo> restore(MainStorageInterface mainStorageInterface, List<DfInfo> dfInfos, AtomicBoolean stopRestoring) throws IOException;

    /**
     * New method not available in original IDS plugin interface that allows 
     * for the progress of restores to be monitored
     * 
     * @return the number of files still to be restored from this restore request
     */
    int getNumFilesRemaining();
    
}
