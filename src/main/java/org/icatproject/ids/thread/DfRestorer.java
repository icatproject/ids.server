package org.icatproject.ids.thread;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Restores datafiles from the slow to the fast storage.
 */
public class DfRestorer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DfRestorer.class);

	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private FiniteStateMachine fsm;

	private List<DfInfo> dfInfos;

	public DfRestorer(List<DfInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dfInfos = dfInfos;
		this.fsm = fsm;

		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();

	}

	/*
	 * For efficiency we expect the archiveStorageInterface to get back as much as possible in one
	 * call. This makes the interface more ugly but is essential in some cases.
	 */
	@Override
	public void run() {

		// for (DfInfo dfInfo : dfInfos) {
		// String dfLocation = dfInfo.getDfLocation(); TODO Should check that it is good
		// }
		try {
			Set<DfInfo> failures = archiveStorageInterface.restore(mainStorageInterface, dfInfos);
			for (DfInfo dfInfo : dfInfos) {
				if (failures.contains(dfInfo)) {
					logger.error("Restore of " + dfInfo + " failed");
				} else {
					logger.debug("Restore of " + dfInfo + " completed");
				}
				fsm.removeFromChanging(dfInfo);
			}
		} catch (IOException e) {
			for (DfInfo dfInfo : dfInfos) {
				logger.error("Restore of " + dfInfo + " failed");
				fsm.removeFromChanging(dfInfo);
			}
			return;
		}

	}
}