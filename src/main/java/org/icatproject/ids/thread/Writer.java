package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.webservice.IdsBean.RequestedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies dataset from main to archive
 */
public class Writer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(Writer.class);
	private DsInfo dsInfo;
	private Map<DsInfo, RequestedState> deferredOpsQueue;
	private Set<DsInfo> changing;
	private MainStorageInterface fastStorageInterface;
	private ArchiveStorageInterface slowStorageInterface;
	private Path archDir;

	public Writer(DsInfo dsInfo, PropertyHandler ph, Map<DsInfo, RequestedState> deferredOpsQueue,
			Set<DsInfo> changing) {
		this.dsInfo = dsInfo;
		this.deferredOpsQueue = deferredOpsQueue;
		this.changing = changing;
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
				InputStream zipIs = fastStorageInterface.get(dsInfo);
				slowStorageInterface.put(dsInfo, zipIs);
				zipIs.close();
			}

			// logger.info("Write of  " + ds.getLocation() + " succesful");
		} catch (Exception e) {
			logger.error("Write of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			synchronized (deferredOpsQueue) {
				changing.remove(dsInfo);
			}
		}
	}

}
