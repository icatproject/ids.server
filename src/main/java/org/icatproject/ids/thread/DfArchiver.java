package org.icatproject.ids.thread;

import java.util.List;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Removes datafiles from the fast storage (doesn't write them to archive storage)
 */
public class DfArchiver implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(DfArchiver.class);

	private MainStorageInterface mainStorageInterface;
	private FiniteStateMachine fsm;
	private List<DfInfo> dfInfos;

	public DfArchiver(List<DfInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dfInfos = dfInfos;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
	}

	@Override
	public void run() {
		for (DfInfo dfInfo : dfInfos) {
			try {
				String dfLocation = dfInfo.getDfLocation();
				mainStorageInterface.delete(dfLocation);
				logger.debug("Archive of " + dfInfo + " completed");
			} catch (Exception e) {
				logger.error("Archive of " + dfInfo + " failed due to " + e.getClass() + " "
						+ e.getMessage());
			} finally {
				fsm.removeFromChanging(dfInfo);
			}
		}
	}
}
