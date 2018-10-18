package org.icatproject.ids.thread;

import java.util.Collection;
import java.util.List;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delete datafiles from archive
 */
public class DfDeleter implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DfDeleter.class);

	private FiniteStateMachine fsm;
	private ArchiveStorageInterface archiveStorageInterface;
	private List<DfInfo> dfInfos;
	private Collection<Lock> locks;

	public DfDeleter(List<DfInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm, Collection<Lock> locks) {
		this.dfInfos = dfInfos;
		this.fsm = fsm;
		this.locks = locks;
		archiveStorageInterface = propertyHandler.getArchiveStorage();
	}

	@Override
	public void run() {
		try {
			for (DfInfo dfInfo : dfInfos) {
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
			for (Lock l: locks) {
				l.release();
			}
		}
	}
}
