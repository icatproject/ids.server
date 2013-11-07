package org.icatproject.ids.thread;

import java.nio.file.Files;
import java.nio.file.Path;

import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.webservice.FiniteStateMachine;
import org.icatproject.ids.webservice.PropertyHandler;
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
	private Path datasetCache;

	public Archiver(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
	}

	@Override
	public void run() {

		try {
			String preparedId;
			// Is the dataset in a preparation?
			if ((preparedId = fsm.preparing(dsInfo)) != null) {
				logger.debug("Archive of " + dsInfo + " skipped as needed by prepare " + preparedId);
			} else {
				// remove any files from the dataset cache
				Path datasetCachePath = datasetCache.resolve(dsInfo.getFacilityName())
						.resolve(dsInfo.getInvName()).resolve(dsInfo.getVisitId())
						.resolve(dsInfo.getDsName());

				Files.deleteIfExists(datasetCachePath);

				mainStorageInterface.delete(dsInfo);
				logger.debug("Archive of " + dsInfo + " completed");
			}
		} catch (Exception e) {
			logger.error("Archive of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}
