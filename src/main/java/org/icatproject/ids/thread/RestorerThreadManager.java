package org.icatproject.ids.thread;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ejb.Singleton;

import org.icatproject.ids.CompletedRestoresManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.RestoreFileCountManager;
import org.icatproject.ids.plugin.DfInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage the restore threads that work on restoring the requested 
 * files for each prepared ID from the Archive Storage system.
 * 
 * All threads are kept in a Map keyed on the prepared ID where the entries
 * are a list of Threads that are working on that prepared ID.
 * 
 * Allowing for a list of Threads per preparedId gives options to limit the
 * maximum number of files per restore thread, either to avoid excessively
 * large requests or to create multiple parallel Threads for potentially
 * increased performance. These need to be worked out from the performance 
 * characteristics of the underlying Archive Storage system.
 */
@Singleton
public class RestorerThreadManager {

	private static Logger logger = LoggerFactory.getLogger(RestorerThreadManager.class);

    private PropertyHandler propertyHandler = PropertyHandler.getInstance();
    private RestoreFileCountManager restoreFileCountManager = RestoreFileCountManager.getInstance();

    private Map<String, List<RestorerThread>> restorerThreadMap = new ConcurrentHashMap<>();

    /**
     * Create a thread to restore the given list of files from Archive Storage.
     * Firstly the list is checked to remove any files that are already on Main
     * Storage. Then a thread is started to carry out the restore and it is put
     * into a Map containing Lists of Threads working on each preparedId.
     * 
     * TODO: make this method create multiple threads to limit the number of 
     * files that can be requested from Archive Storage in each request.
     * 
     * @param preparedId
     * @param dfInfosToRestore
     */
    public void createRestorerThread(String preparedId, List<DfInfo> dfInfosToRestore) {
        List<DfInfo> dfInfosNeedingRestore = getDfInfosNeedingRestore(dfInfosToRestore);
        if (!dfInfosNeedingRestore.isEmpty()) {
            // delete a completed file if it exists
            CompletedRestoresManager completedRestoresManager = 
                    new CompletedRestoresManager(propertyHandler.getCacheDir());
            completedRestoresManager.deleteCompletedFile(preparedId);
            // start up a thread to process the restore
            RestorerThread restorerThread = new RestorerThread(this, preparedId, dfInfosNeedingRestore);
            List<RestorerThread> restorerThreads = restorerThreadMap.get(preparedId);
            if (restorerThreads == null) {
                // create a new list and add it to the map
                restorerThreads = new ArrayList<>();
                restorerThreadMap.put(preparedId, restorerThreads);
            }
            restorerThreads.add(restorerThread);
            restorerThread.start();
        }
    }

    /**
     * Take a list of DatafileInfo objects that has been requested and check on 
     * main storage (IDS cache) to see which already exist and therefore don't 
     * need to be requested from archive storage (StorageD/tape).
     * 
     * @param dfInfosToRestore the list of DataFileInfos requested
     * @return a potentially shorter list having removed any that were found to
     *         already be on main storage
     */
    private List<DfInfo> getDfInfosNeedingRestore(List<DfInfo> dfInfosToRestore) {
        List<DfInfo> dfInfosOnMainStorage = new ArrayList<>();
        for (DfInfo dfInfo : dfInfosToRestore) {
            if (propertyHandler.getMainStorage().exists(dfInfo.getDfLocation())) {
                dfInfosOnMainStorage.add(dfInfo);
            }
        }
        logger.debug("Found {}/{} files requested already on main storage", 
                dfInfosOnMainStorage.size(), dfInfosToRestore.size());
        dfInfosToRestore.removeAll(dfInfosOnMainStorage);
        return dfInfosToRestore;
    }

    /**
     * Get the total number of files that are still to be restored from the 
     * Archive Storage for the given preparedId.
     * 
     * This is done by totalling up the number of files remaining on each of
     * the threads working on the given preparedId.
     * 
     * @param preparedId the prepared ID
     * @return the total number of files still to be restored
     */
    public int getTotalNumFilesRemaining(String preparedId) {
        List<RestorerThread> restorerThreads = restorerThreadMap.get(preparedId);
        if (restorerThreads != null) {
            int totalFilesRemaining = 0;
            for (RestorerThread restorerThread : restorerThreads) {
                totalFilesRemaining += restorerThread.getNumFilesRemaining();
            }
            return totalFilesRemaining;
        } else {
            logger.warn("Entry for preparedId {} not found in map", preparedId);
            return 0;
        }
    }

    /**
     * Remove a thread (usually on completion) from the List of threads being 
     * managed for the given preparedId. 
     * If this is the last thread running for the given preparedId then the 
     * list will now be empty, so remove the entry for the preparedId from the
     * Map as well and create a "completed" file to record that the restore has
     * completed.
     *  
     * @param preparedId the prepared ID
     * @param restorerThread the thread to remove from the Map of threads being
     *                       managed
     */
    public void removeThreadFromMap(String preparedId, RestorerThread restorerThread) {
        logger.info("Removing thread for preparedId {} from list", preparedId);
        List<RestorerThread> restorerThreads = restorerThreadMap.get(preparedId);
        restorerThreads.remove(restorerThread);
        if (restorerThreads.size() == 0) {
            logger.info("Removing preparedId {} entry from map", preparedId);
            restorerThreadMap.remove(preparedId);
            // write a completed file
            CompletedRestoresManager completedRestoresManager = 
                    new CompletedRestoresManager(propertyHandler.getCacheDir());
            completedRestoresManager.createCompletedFile(preparedId);
            // remove the fileCount entry from the map in RestoreFileCountManager
            restoreFileCountManager.removeEntryFromMap(preparedId);
        }
    }

}
