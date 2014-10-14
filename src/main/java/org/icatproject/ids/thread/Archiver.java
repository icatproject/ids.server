package org.icatproject.ids.thread;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Removes datasets from the fast storage (doesn't write them to slow storage)
 */
public class Archiver implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(Archiver.class);
	private DsInfo dsInfo;

	private MainStorageInterface mainStorageInterface;
	private FiniteStateMachine fsm;

	public Archiver(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
	}

	@Override
	public void run() {
		try {
			mainStorageInterface.delete(dsInfo);
			logger.debug("Archive of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Archive of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}
