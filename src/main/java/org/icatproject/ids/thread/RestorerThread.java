package org.icatproject.ids.thread;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.icatproject.ids.FailedFilesManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.storage.ArchiveStorageInterfaceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestorerThread extends Thread {

	private static Logger logger = LoggerFactory.getLogger(RestorerThread.class);

    private PropertyHandler propertyHandler = PropertyHandler.getInstance();

    private RestorerThreadManager restorerThreadManager;
    private String preparedId;
    private List<DfInfo> dfInfosToRestore;
    private ArchiveStorageInterfaceV2 archiveStorage;
    private AtomicBoolean stopRestoring;

    public RestorerThread(RestorerThreadManager restorerThreadManager, 
                          String preparedId, List<DfInfo> dfInfosToRestore) throws InternalException {
        this.restorerThreadManager = restorerThreadManager;
        this.preparedId = preparedId;
        this.dfInfosToRestore = dfInfosToRestore;
        this.stopRestoring = new AtomicBoolean(false);
        Class<ArchiveStorageInterfaceV2> archiveStorageClass = propertyHandler.getArchiveStorageClass();
        try {
            archiveStorage = archiveStorageClass.getConstructor(Properties.class).newInstance(propertyHandler.getSimpleProperties());
            logger.debug("Successfully instantiated {} for {}", archiveStorageClass.getName(), preparedId);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            // This should never happen because a test for instantiation of 
            // this class is done in the PropertyHandler. 
            String message = "Failed to create instance of ArchiveStorageInterfaceV2 class " + archiveStorageClass.getName();
            logger.error(message, e);
            throw new InternalException(message);
        }
    }

    @Override
    public void run() {
        try {
            Set<DfInfo> failedRestores = archiveStorage.restore(propertyHandler.getMainStorage(), dfInfosToRestore, stopRestoring);
            if (stopRestoring.get()) {
                // this thread has been requested to stop
                // return here and the thread will end
                logger.info("Exiting restore thread for prepared ID {}", preparedId);
                return;
            }
            // call FailedFilesManager to get these failures written to the "failed" file
            Set<String> failedFilepaths = new TreeSet<>();
            for (DfInfo dfInfo : failedRestores) {
                failedFilepaths.add(dfInfo.getDfLocation());
            }
            FailedFilesManager failedFilesManager = new FailedFilesManager(propertyHandler.getCacheDir());
            failedFilesManager.writeToFailedEntriesFile(preparedId, failedFilepaths);
        } catch (IOException e) {
            // this was thrown by the restore method and is likely to be a transient
            // connection problem so resubmit the request to try again
            // resubmit the list of DfInfos to try again in another thread
            logger.error("IOException for preparedId ID " + preparedId + ". Recreating restorer thread.", e);
            try {
                restorerThreadManager.createRestorerThread(preparedId, dfInfosToRestore, true);
            } catch (InternalException e1) {
                logger.error("Error recreating RestorerThread: {}", e.getMessage());
            }
        }
        restorerThreadManager.removeThreadFromMap(preparedId, this);
        logger.debug("RestorerThread finishing for preparedId {}", preparedId);
    }

    public int getNumFilesRemaining() {
        return archiveStorage.getNumFilesRemaining();
    }

    /**
     * Set the flag which tells the archive storage to stop restoring files
     * at the next opportunity
     */
    public void setStopRestoring() {
        logger.info("Setting stop restoring flag for prepared ID {}", preparedId);
        stopRestoring.set(true);
    }

}
