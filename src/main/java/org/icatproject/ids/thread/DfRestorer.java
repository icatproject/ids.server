package org.icatproject.ids.thread;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;

/*
 * Restores datafiles from the slow to the fast storage.
 */
public class DfRestorer implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(DfRestorer.class);

    private MainStorageInterface mainStorageInterface;
    private ArchiveStorageInterface archiveStorageInterface;
    private FiniteStateMachine fsm;
    private List<DfInfo> dfInfos;
    private Collection<Lock> locks;

    public DfRestorer(List<DfInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm, Collection<Lock> locks) {
        this.dfInfos = dfInfos;
        this.fsm = fsm;
        this.locks = locks;

        mainStorageInterface = propertyHandler.getMainStorage();
        archiveStorageInterface = propertyHandler.getArchiveStorage();

    }

    /*
     * For efficiency we expect the archiveStorageInterface to get back as much
     * as possible in one call. This makes the interface more ugly but is
     * essential in some cases.
     */
    @Override
    public void run() {
        try {
            /*
             * This code avoids unnecessary calls to restore files. It will not
             * generally remove anything from the list of files to restore as
             * pointless restores are normally filtered out earlier.
             */
            Iterator<DfInfo> iter = dfInfos.iterator();
            while (iter.hasNext()) {
                DfInfo dfInfo = iter.next();
                if (mainStorageInterface.exists(dfInfo.getDfLocation())) {
                    iter.remove();
                    fsm.removeFromChanging(dfInfo);
                }
            }

            Set<DfInfo> failures = archiveStorageInterface.restore(mainStorageInterface, dfInfos);
            for (DfInfo dfInfo : dfInfos) {
                if (failures.contains(dfInfo)) {
                    fsm.recordFailure(dfInfo.getDfId());
                    logger.error("Restore of " + dfInfo + " failed");
                } else {
                    fsm.recordSuccess(dfInfo.getDfId());
                    logger.debug("Restore of " + dfInfo + " completed");
                }
                fsm.removeFromChanging(dfInfo);
            }
        } catch (Exception e) {
            for (DfInfo dfInfo : dfInfos) {
                logger.error("Restore of " + dfInfo + " failed " + e.getClass() + " " + e.getMessage());
                fsm.removeFromChanging(dfInfo);
            }
        } finally {
            for (Lock l : locks) {
                l.release();
            }
        }
    }
}
