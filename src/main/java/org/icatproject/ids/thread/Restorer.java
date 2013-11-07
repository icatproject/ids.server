package org.icatproject.ids.thread;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Restores datafiles from the slow to the fast storage.
 */
public class Restorer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(Restorer.class);

	private DsInfo dsInfo;

	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private FiniteStateMachine fsm;

	private Path datasetCache;

	public Restorer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
	}

	@Override
	public void run() {
		try {
			// Get the file into the dataset cache
			Path dir = datasetCache.resolve(dsInfo.getFacilityName()).resolve(dsInfo.getInvName())
					.resolve(dsInfo.getVisitId());
			Files.createDirectories(dir);
			Path tPath = Files.createTempFile(dir, null, null);
			Files.copy(archiveStorageInterface.get(dsInfo), tPath,
					StandardCopyOption.REPLACE_EXISTING);
			Path path = dir.resolve(dsInfo.getDsName());
			Files.move(tPath, path, StandardCopyOption.ATOMIC_MOVE,
					StandardCopyOption.REPLACE_EXISTING);

			// Now split file and store it locally
			ZipInputStream zis = new ZipInputStream(Files.newInputStream(path));
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String dfName = new File(ze.getName()).toPath().getFileName().toString();
				mainStorageInterface.putUnchecked(dsInfo, dfName, zis);
				ze = zis.getNextEntry();
			}
			zis.close();
			logger.debug("Restore of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Restore of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}