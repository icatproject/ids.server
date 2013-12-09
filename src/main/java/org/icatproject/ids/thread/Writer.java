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
	private Path markerDir;
	private boolean compress;
	private IcatReader reader;

	public Writer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm,
			IcatReader reader) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
		markerDir = propertyHandler.getCacheDir().resolve("marker");
		compress = propertyHandler.isCompressDatasetCache();
		this.reader = reader;
	}

	@Override
	public void run() {
		try {
			Path datasetCachePath = datasetCache.resolve(Long.toString(dsInfo.getInvId())).resolve(
					Long.toString(dsInfo.getDsId()));

			if (!mainStorageInterface.exists(dsInfo)) {
				logger.info("No files present for " + dsInfo + " - archive deleted");
				Files.deleteIfExists(datasetCachePath);
				mainStorageInterface.delete(dsInfo);
				archiveStorageInterface.delete(dsInfo);
			} else {
				if (!Files.exists(datasetCachePath)) {
					logger.debug("Creating " + datasetCachePath);
					Files.createDirectories(datasetCachePath.getParent());
					List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile",
							dsInfo.getDsId())).getDatafiles();
					ZipOutputStream zos = new ZipOutputStream(
							Files.newOutputStream(datasetCachePath));
					if (!compress) {
						zos.setLevel(0);
					}
					for (Datafile datafile : datafiles) {
						zos.putNextEntry(new ZipEntry("ids/" + dsInfo.getFacilityName() + "/"
								+ dsInfo.getInvName() + "/" + dsInfo.getVisitId() + "/"
								+ dsInfo.getDsName() + "/" + datafile.getName()));
						InputStream is = mainStorageInterface.get(datafile.getLocation(),
								datafile.getCreateId());
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
			logger.debug("Write of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Write of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}
