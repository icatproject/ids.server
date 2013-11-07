package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Copies datasets across storages (fast to slow) and subsequently
 * removes the files on the fast storage
 */
public class WriteThenArchiver implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(WriteThenArchiver.class);

	private static final int BUFSIZ = 1024;

	private DsInfo dsInfo;

	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private FiniteStateMachine fsm;

	private Path datasetCache;

	public WriteThenArchiver(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
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
				archiveStorageInterface.delete(dsInfo);
			} else {
				if (!Files.exists(datasetCachePath)) {
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
			Files.deleteIfExists(datasetCachePath);
			mainStorageInterface.delete(dsInfo);
			logger.debug("Write then archive of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Write then archive of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}
