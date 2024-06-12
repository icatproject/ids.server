package org.icatproject.ids.thread;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.models.DatafileInfo;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.services.LockManager.Lock;
import org.icatproject.ids.services.PropertyHandler;

/*
 * Restores datafiles from the slow to the fast storage.
 */
public class DfRestorer implements Runnable {

    private final static Logger logger = LoggerFactory
            .getLogger(DfRestorer.class);

    private MainStorageInterface mainStorageInterface;
    private ArchiveStorageInterface archiveStorageInterface;
    private FiniteStateMachine fsm;
    private List<DatafileInfo> dataFileInfos;
    private Collection<Lock> locks;

    public DfRestorer(List<DatafileInfo> dfInfos,
            PropertyHandler propertyHandler, FiniteStateMachine fsm,
            Collection<Lock> locks) {
        this.dataFileInfos = dfInfos;
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
            Iterator<DatafileInfo> iter = dataFileInfos.iterator();
            while (iter.hasNext()) {
                DatafileInfo dfInfo = iter.next();
                if (mainStorageInterface.exists(dfInfo.getDfLocation())) {
                    iter.remove();
                    fsm.removeFromChanging(dfInfo);
                }
            }

            // TODO: This is additional conversion caused by the redesign :-(
            List<DfInfo> dfInfos = new ArrayList<>();
            for (DfInfo dfInfo : this.dataFileInfos) {
                dfInfos.add(dfInfo);
            }

            Set<DfInfo> failures = archiveStorageInterface
                    .restore(mainStorageInterface, dfInfos);
            for (DatafileInfo dfInfo : dataFileInfos) {
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
            for (DatafileInfo dfInfo : dataFileInfos) {
                logger.error("Restore of " + dfInfo + " failed " + e.getClass()
                        + " " + e.getMessage());
                fsm.removeFromChanging(dfInfo);
            }
        } finally {
            for (Lock l : locks) {
                l.release();
            }
        }
    }
}
