package org.icatproject.ids.thread;

import java.nio.file.Files;
import java.nio.file.Path;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.webservice.FiniteStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies dataset from main to archive
 */
public class Writer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(Writer.class);
	private DsInfo dsInfo;

	private MainStorageInterface fastStorageInterface;
	private ArchiveStorageInterface slowStorageInterface;
	private Path archDir;
	private FiniteStateMachine fsm;

	public Writer(DsInfo dsInfo, PropertyHandler ph, FiniteStateMachine fsm) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		fastStorageInterface = ph.getMainStorage();
		slowStorageInterface = ph.getArchiveStorage();
		archDir = ph.getCacheDir().resolve("dataset");
	}

	@Override
	public void run() {
		try {
			Path localDatasetDir = archDir.resolve(Long.toString(dsInfo.getInvId()));
			if (!fastStorageInterface.exists(dsInfo)) {
				logger.info("No files present for " + dsInfo + " - archive deleted");
				slowStorageInterface.delete(dsInfo);
				Files.deleteIfExists(localDatasetDir.resolve(dsInfo.getDsName() + ".zip"));
			} else {
				Files.createDirectories(localDatasetDir);
				// TODO fix this
				// InputStream zipIs = fastStorageInterface.get(dsInfo);
				// slowStorageInterface.put(dsInfo, zipIs);
				// zipIs.close();
			}

			// logger.info("Write of  " + ds.getLocation() + " succesful");
		} catch (Exception e) {
			logger.error("Write of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
			
		}
	}

}
