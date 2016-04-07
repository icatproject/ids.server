package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies datafiles from main to archive
 */
public class DfWriter implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DfWriter.class);

	private FiniteStateMachine fsm;
	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;

	private Path markerDir;

	private List<DfInfo> dfInfos;

	public DfWriter(List<DfInfo> dfInfos, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dfInfos = dfInfos;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		markerDir = propertyHandler.getCacheDir().resolve("marker");
	}

	@Override
	public void run() {
		for (DfInfo dfInfo : dfInfos) {
			String dfLocation = dfInfo.getDfLocation();
			try (InputStream is = mainStorageInterface.get(dfLocation, dfInfo.getCreateId(), dfInfo.getModId())) {
				archiveStorageInterface.put(is, dfLocation);
				Path marker = markerDir.resolve(Long.toString(dfInfo.getDfId()));
				Files.deleteIfExists(marker);
				logger.debug("Removed marker " + marker);
				logger.debug("Write of " + dfInfo + " completed");
			} catch (Exception e) {
				logger.error("Write of " + dfInfo + " failed due to " + e.getClass() + " "
						+ e.getMessage());
			} finally {
				fsm.removeFromChanging(dfInfo);
			}
		}

	}
}
