package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.IcatReader;
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
	private Path markerDir;

	private IcatReader reader;

	public WriteThenArchiver(DsInfo dsInfo, PropertyHandler propertyHandler,
			FiniteStateMachine fsm, IcatReader reader) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
		markerDir = propertyHandler.getCacheDir().resolve("marker");
		this.reader = reader;
	}

	@Override
	public void run() {

		try {
			Path datasetCachePath = datasetCache.resolve(Long.toString(dsInfo.getInvId())).resolve(
					Long.toString(dsInfo.getDsId()));

			if (!mainStorageInterface.exists(dsInfo)) {
				logger.info("No files present for " + dsInfo + " - archive deleted");
				archiveStorageInterface.delete(dsInfo);
			} else {
				if (!Files.exists(datasetCachePath)) {
					Files.createDirectories(datasetCachePath.getParent());
					List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile",
							dsInfo.getDsId())).getDatafiles();
					ZipOutputStream zos = new ZipOutputStream(
							Files.newOutputStream(datasetCachePath));
					zos.setLevel(0);
					for (Datafile datafile : datafiles) {
						String location = datafile.getLocation();
						zos.putNextEntry(new ZipEntry("ids/" + dsInfo.getFacilityName() + "/"
								+ dsInfo.getInvName() + "/" + dsInfo.getVisitId() + "/"
								+ dsInfo.getDsName() + "/" + datafile.getName()));
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
			Path marker = markerDir.resolve(Long.toString(dsInfo.getDsId()));
			Files.deleteIfExists(marker);
			logger.debug("Removed marker " + marker);
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
