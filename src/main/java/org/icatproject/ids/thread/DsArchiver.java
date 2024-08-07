package org.icatproject.ids.thread;

import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.models.DatasetInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.services.PropertyHandler;
import org.icatproject.ids.services.LockManager.Lock;

/*
 * Removes datasets from the fast storage (doesn't write them to slow storage)
 */
public class DsArchiver implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(DsArchiver.class);
    private DatasetInfo dsInfo;

    private MainStorageInterface mainStorageInterface;
    private FiniteStateMachine fsm;
    private Path markerDir;
    private Lock lock;

    public DsArchiver(DatasetInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm, Lock lock) {
        this.dsInfo = dsInfo;
        this.fsm = fsm;
        mainStorageInterface = propertyHandler.getMainStorage();
        markerDir = propertyHandler.getCacheDir().resolve("marker");
        this.lock = lock;
    }

    @Override
    public void run() {
        try {
            if (Files.exists(markerDir.resolve(Long.toString(dsInfo.getDsId())))) {
                logger.error("Archive of " + dsInfo
                        + " not carried out because a write to secondary storage operation failed previously");
            } else {
                mainStorageInterface.delete(dsInfo);
                logger.debug("Archive of " + dsInfo + " completed");
            }
        } catch (Exception e) {
            logger.error("Archive of " + dsInfo + " failed due to " + e.getMessage());
        } finally {
            fsm.removeFromChanging(dsInfo);
            lock.release();
        }
    }
}
