package org.icatproject.ids.thread;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

/*
 * Removes datasets from the fast storage (doesn't write them to slow storage)
 */
public class DsArchiver implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(DsArchiver.class);
    private DsInfo dsInfo;

    private MainStorageInterface mainStorageInterface;
    private FiniteStateMachine fsm;
    private Path markerDir;
    private Lock lock;

    public DsArchiver(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm, Lock lock) {
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
