package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.webservice.FiniteStateMachine;
import org.icatproject.ids.webservice.PropertyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies dataset from main to archive
 */
public class Writer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(Writer.class);
	private static final int BUFSIZ = 1024;
	private DsInfo dsInfo;

	private FiniteStateMachine fsm;
	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private Path datasetCache;

	public Writer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
	}

	@Override
	public void run() {
		try {
			Path datasetCachePath = datasetCache.resolve(dsInfo.getFacilityName())
					.resolve(dsInfo.getInvName()).resolve(dsInfo.getVisitId())
					.resolve(dsInfo.getDsName());

			if (!mainStorageInterface.exists(dsInfo)) {
				logger.info("No files present for " + dsInfo + " - archive deleted");
				Files.deleteIfExists(datasetCachePath);
				mainStorageInterface.delete(dsInfo);
				archiveStorageInterface.delete(dsInfo);
			} else {
				if (!Files.exists(datasetCachePath)) {
					logger.debug("Creating " + datasetCachePath);
					Files.createDirectories(datasetCachePath.getParent());
					List<String> locations = mainStorageInterface.getLocations(dsInfo);
					ZipOutputStream zos = new ZipOutputStream(
							Files.newOutputStream(datasetCachePath));
					zos.setLevel(0);
					for (String location : locations) {
						zos.putNextEntry(new ZipEntry("ids/" + location));
						InputStream is = mainStorageInterface.get(location);
						int bytesRead = 0;
						byte[] buffer = new byte[BUFSIZ];
						while ((bytesRead = is.read(buffer)) > 0) {
							zos.write(buffer, 0, bytesRead);
						}
						zos.closeEntry();
					}
					zos.close();
				}
				archiveStorageInterface.put(dsInfo, Files.newInputStream(datasetCachePath));
			}

			logger.debug("Write of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Write of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
			// TODO add non-volatile write guarantee
		}
	}

}
