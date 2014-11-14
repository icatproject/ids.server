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

/*
 * Copies datasets across storages (fast to slow) and subsequently
 * removes the files on the fast storage
 */
public class DfWriteThenArchiver implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DfWriteThenArchiver.class);

	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private FiniteStateMachine fsm;

	private Path markerDir;

	private List<DfInfo> dfInfos;

	public DfWriteThenArchiver(List<DfInfo> dfInfos, PropertyHandler propertyHandler,
			FiniteStateMachine fsm) {
		this.dfInfos = dfInfos;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		markerDir = propertyHandler.getCacheDir().resolve("marker");
	}

	@Override
	public void run() {

		for (DfInfo dfInfo : dfInfos) {
			try {
				String dfLocation = dfInfo.getDfLocation();
				InputStream is = mainStorageInterface.get(dfLocation, dfInfo.getCreateId(),
						dfInfo.getModId());
				archiveStorageInterface.put(is, dfLocation);
				mainStorageInterface.delete(dfLocation, dfInfo.getCreateId(), dfInfo.getModId());
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
