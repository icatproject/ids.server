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
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.storage.ArchiveStorageInterfaceDLS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestorerThread extends Thread {

	private static Logger logger = LoggerFactory.getLogger(RestorerThread.class);

    private PropertyHandler propertyHandler = PropertyHandler.getInstance();

    private RestorerThreadManager restorerThreadManager;
    private String preparedId;
    private List<DfInfo> dfInfosToRestore;
    private ArchiveStorageInterfaceDLS archiveStorage;
    private AtomicBoolean stopRestoring;

    public RestorerThread(RestorerThreadManager restorerThreadManager, 
                          String preparedId, List<DfInfo> dfInfosToRestore) {
        this.restorerThreadManager = restorerThreadManager;
        this.preparedId = preparedId;
        this.dfInfosToRestore = dfInfosToRestore;
        this.stopRestoring = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        Class<ArchiveStorageInterfaceDLS> archiveStorageClass = propertyHandler.getArchiveStorageClass();
        try {
            archiveStorage = archiveStorageClass.getConstructor(Properties.class).newInstance(propertyHandler.getSimpleProperties());
            logger.debug("Successfully instantiated {} for {}", archiveStorageClass.getName(), preparedId);
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
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            // This should never happen because a test for instantiation of 
            // this class is done in the PropertyHandler. 
            // If this does happen there is a serious problem that needs 
            // intervention so log it and let the thread complete. 
            // There is no point resubmitting the request.         
            logger.error("Failed to create instance of ArchiveStorageInterfaceDLS class " + archiveStorageClass.getName(), e);
        } catch (IOException e) {
            // this was thrown by the restore method and is likely to be a transient
            // connection problem so resubmit the request to try again
            // resubmit the list of DfInfos to try again in another thread
            logger.error("IOException for preparedId ID " + preparedId + ". Recreating restorer thread.", e);
            restorerThreadManager.createRestorerThread(preparedId, dfInfosToRestore, true);
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
