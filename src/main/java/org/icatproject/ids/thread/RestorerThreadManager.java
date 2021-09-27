package org.icatproject.ids.thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PreDestroy;
import javax.ejb.Singleton;

import org.icatproject.ids.CompletedRestoresManager;
import org.icatproject.ids.FailedFilesManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.RestoreFileCountManager;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
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
 * Allowing for a list of Threads per preparedId makes it possible to limit the
 * maximum number of files per restore thread, either to avoid excessively
 * large requests or to create multiple parallel Threads for potentially
 * increased performance. These need to be worked out from the performance 
 * characteristics of the underlying Archive Storage system.
 */
@Singleton
public class RestorerThreadManager {

	private static Logger logger = LoggerFactory.getLogger(RestorerThreadManager.class);

    private PropertyHandler propertyHandler;
    private RestoreFileCountManager restoreFileCountManager;
    private Map<String, List<RestorerThread>> restorerThreadMap;


    public RestorerThreadManager() {
        propertyHandler = PropertyHandler.getInstance();
        restoreFileCountManager = RestoreFileCountManager.getInstance();
        restorerThreadMap = new ConcurrentHashMap<>();
    }

    /**
     * Submit a list of DatafileInfo objects corresponding to the files that 
     * have been requested for restoration from Archive Storage. Firstly the 
     * Main Storage cache is checked to remove any files that are already 
     * available, then if the number of files exceeds the limit per restore 
     * request then the request is split into multiple requests that are all
     * below the allowed limit.
     * 
     * @param preparedId the prepared ID relating to the restore request
     * @param dfInfosToRestore the list of DatafileInfo objects to restore
     * @param checkCache whether to check if the files are already on the cache
     * @throws InternalException if there is a problem creating a RestorerThread
     */
    public void submitFilesForRestore(String preparedId, List<DfInfo> dfInfosToRestore, boolean checkCache) throws InternalException {
        logger.debug("{} files submitted for restore with preparedId {}", dfInfosToRestore.size(), preparedId);
        if (checkCache) {
            removeFilesAlreadyOnCache(dfInfosToRestore);
        }
        if (dfInfosToRestore.isEmpty()) {
            logger.debug("All files are already on cache for preparedId {}", preparedId);
            // write a completed file
            CompletedRestoresManager completedRestoresManager = 
                    new CompletedRestoresManager(propertyHandler.getCacheDir());
            completedRestoresManager.createCompletedFile(preparedId);
            // write a failed files file (empty)
            FailedFilesManager failedFilesManager = 
                    new FailedFilesManager(propertyHandler.getCacheDir());
            failedFilesManager.writeToFailedEntriesFile(preparedId, Collections.emptySet());
        } else {
            logger.debug("{} files will be restored for preparedId {}", dfInfosToRestore.size(), preparedId);
            int maxFilesPerRequest = propertyHandler.getMaxRestoresPerThread();
            int numFilesToRestore = dfInfosToRestore.size();
            int numThreads = (numFilesToRestore/maxFilesPerRequest);
            if (numFilesToRestore%maxFilesPerRequest != 0) {
                numThreads += 1;
            }
            logger.debug("{} restore thread will be created for preparedId {}", numThreads, preparedId);
            int filesPerRequest = ((numFilesToRestore-1)/numThreads) + 1;
            logger.debug("The max number of files per restore thread for preparedId {} will be {}", preparedId, filesPerRequest);

            int fromIndex = 0;
            for (int threadCount=1; threadCount<=numThreads; threadCount++) {
                int toIndex = fromIndex + filesPerRequest;
                if (toIndex > numFilesToRestore) {
                    toIndex = numFilesToRestore;
                }
                List<DfInfo> subList = dfInfosToRestore.subList(fromIndex, toIndex);
                createRestorerThread(preparedId, subList, false);
                fromIndex = toIndex;
            }
        }
    }

    /**
     * Create a thread to restore the given list of files from Archive Storage.
     * A thread is started to carry out the restore and it is put into a Map 
     * containing Lists of Threads working on each preparedId.
     * 
     * Note that, in general, this method should not be called directly from 
     * other classes, but the submitFilesForRestore method should be used 
     * instead as this ensures that the maxRestoresPerThread limit is being
     * used. However, in some cases it may be more efficient to call this
     * method directly.
     * 
     * @param preparedId the prepared ID that this request relates to
     * @param dfInfosToRestore the list of DatafileInfo objects to restore
     * @param checkCache whether to check if the files are already on the cache
     * @throws InternalException if there is a problem creating a RestoreThread
     */
    public void createRestorerThread(String preparedId, List<DfInfo> dfInfosToRestore, boolean checkCache) throws InternalException {
        if (checkCache) {
            removeFilesAlreadyOnCache(dfInfosToRestore);
        }
        if (!dfInfosToRestore.isEmpty()) {
            // delete a completed file if it exists
            CompletedRestoresManager completedRestoresManager = 
                    new CompletedRestoresManager(propertyHandler.getCacheDir());
            completedRestoresManager.deleteCompletedFile(preparedId);
            // start up a thread to process the restore
            RestorerThread restorerThread = new RestorerThread(this, preparedId, dfInfosToRestore);
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
     * Remove all of the DfInfo objects corresponding to files already on the 
     * cache from the list to be restored.
     * 
     * Note that the list passed in (by reference) is modified and there is no
     * return value.
     * 
     * @param dfInfosToRestore the list of DataFileInfos requested
     */
    private void removeFilesAlreadyOnCache(List<DfInfo> dfInfosToRestore) {
        List<DfInfo> dfInfosOnMainStorage = new ArrayList<>();
        for (DfInfo dfInfo : dfInfosToRestore) {
            if (propertyHandler.getMainStorage().exists(dfInfo.getDfLocation())) {
                dfInfosOnMainStorage.add(dfInfo);
            }
        }
        logger.debug("Found {}/{} files requested already on main storage", 
                dfInfosOnMainStorage.size(), dfInfosToRestore.size());
        dfInfosToRestore.removeAll(dfInfosOnMainStorage);
    }

    /**
     * Get the total number of files that are still to be restored from the 
     * Archive Storage for the given preparedId.
     * 
     * This is done by totalling up the number of files remaining on each of
     * the threads working on the given preparedId.
     * 
     * If there are no restorer threads running for the given prepared ID then
     * zero is returned.
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

    /**
     * Return a string indicating the percentage of files that are already on
     * main storage (cache) for the given prepared ID.
     * 
     * The return value is a string to allow for the possibility of returning
     * values other than integers between 0 and 100, such as "UNKNOWN".
     * 
     * @param preparedId the prepared ID
     * @return where possible a number between 0 and 100 but also perhaps 
     *         a string indicating another status
     * @throws InternalException if the "prepared" file needed to be read and 
     *                           there was a problem reading it.
     * @throws NotFoundException if the "prepared" file needed to be read to 
     *                           find the total number of files and could not
     *                           be found.
     */
    public String getPercentageComplete(String preparedId) throws InternalException, NotFoundException {
        int totalFileCount = restoreFileCountManager.getFileCount(preparedId);
        int filesRemaining = getTotalNumFilesRemaining(preparedId);
        int filesRestored = totalFileCount - filesRemaining;
        int percentageComplete = (filesRestored*100)/totalFileCount;
        return String.valueOf(percentageComplete);
    }

    /**
     * Construct a string that gives an overview of the status of all of the 
     * restore threads being managed.
     * 
     * @return a string summarising the status
     */
    public String getStatusString() {
        if (restorerThreadMap.size() == 0) {
            return "There are no restores in progress";
        }
        String statusString = "";
        for (String preparedId : restorerThreadMap.keySet()) {
            String percentageComplete;
            try {
                percentageComplete = getPercentageComplete(preparedId);
                statusString += preparedId + " : " + percentageComplete + "%\n";
            } catch (InternalException | NotFoundException e) {
                // ignore this
            }
        }
        return statusString;
    }

    /**
     * Cancel all of the restore threads that are restoring files for the given
     * prepared ID.
     * 
     * This actually sets a flag in each of the restorer threads that in turn
     * is regularly checked by the Archive Storage implementation and will 
     * cause it to stop restoring files if it finds the flag has been set.
     * 
     * @param preparedId the prepared ID
     * @throws NotFoundException if there are no restorer threads running for 
     *                           the given prepared ID
     */
    public void cancelThreadsForPreparedId(String preparedId) throws NotFoundException {
        List<RestorerThread> restorerThreads = restorerThreadMap.get(preparedId);
        if (restorerThreads == null) {
            String message = "No restore threads running for prepared ID " + preparedId;
            logger.error(message);
            throw new NotFoundException(message);
        } else {
            logger.info("Requesting {} restore thread(s) to stop for prepared ID {}", restorerThreads.size(), preparedId);
            // loop over all of the restorer threads for this prepared ID and set the stop flag
            for (RestorerThread restorerThread : restorerThreads) {
                restorerThread.setStopRestoring();
            }
        }
        restorerThreadMap.remove(preparedId);
    }

	@PreDestroy
	public void exit() {
        for (String preparedId : restorerThreadMap.keySet()) {
            try {
                cancelThreadsForPreparedId(preparedId);
            } catch (NotFoundException e) {
                logger.error("Error cancelling restore threads for prepared ID {}. {}", preparedId, e.getMessage());
            }
        }
        logger.info("RestorerThreadManager stopped");
	}


}
