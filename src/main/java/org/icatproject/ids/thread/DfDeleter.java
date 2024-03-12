package org.icatproject.ids.thread;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.models.DataFileInfo;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.services.PropertyHandler;
import org.icatproject.ids.services.LockManager.Lock;

/**
 * Delete datafiles from archive
 */
public class DfDeleter implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(DfDeleter.class);

    private FiniteStateMachine fsm;
    private ArchiveStorageInterface archiveStorageInterface;
    private List<DataFileInfo> dfInfos;
    private Collection<Lock> locks;

    public DfDeleter(List<DataFileInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm, Collection<Lock> locks) {
        this.dfInfos = dfInfos;
        this.fsm = fsm;
        this.locks = locks;
        archiveStorageInterface = propertyHandler.getArchiveStorage();
    }

    @Override
    public void run() {
        try {
            for (DataFileInfo dfInfo : dfInfos) {
                try {
                    String dfLocation = dfInfo.getDfLocation();
                    archiveStorageInterface.delete(dfLocation);
                    logger.debug("Delete of " + dfInfo + " completed");
                } catch (Exception e) {
                    logger.error("Delete of " + dfInfo + " failed due to " + e.getClass() + " " + e.getMessage());
                } finally {
                    fsm.removeFromChanging(dfInfo);
                }
            }
        } finally {
            for (Lock l : locks) {
                l.release();
            }
        }
    }
}
