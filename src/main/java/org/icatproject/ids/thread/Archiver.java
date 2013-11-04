package org.icatproject.ids.thread;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.webservice.FiniteStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Removes datasets from the fast storage (doesn't write them to slow storage)
 */
public class Archiver implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(Archiver.class);
	private DsInfo dsInfo;

	private MainStorageInterface fastStorageInterface;
	private ArchiveStorageInterface slowStorageInterface;
	private FiniteStateMachine fsm;

	public Archiver(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.fsm = fsm;
		fastStorageInterface = propertyHandler.getMainStorage();
		slowStorageInterface = propertyHandler.getArchiveStorage();
	}

	@Override
	public void run() {
		logger.info("starting Archiver");

//		StatusInfo resultingStatus = StatusInfo.COMPLETED; // assuming that everything will go OK

		// try {
		// fastStorageInterface.deleteDataset(dsInfo);
		// for (Datafile df : ds.getDatafiles()) {
		// fastStorageInterface.deleteDatafile(df.getLocation());
		// }
		// logger.info("Archive of  " + dsInfo + " succesful");
		// } catch (Exception e) {
		// logger.error("Archive of " + dsInfo + " failed due to " + e.getMessage());
		// resultingStatus = StatusInfo.INCOMPLETE;
		// } finally {
		// synchronized (deferredOpsQueue) {
		// logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
		// requestHelper.setDataEntityStatus(de, resultingStatus);
		// changing.remove(dsInfo);
		// }
		// }
	}
}
