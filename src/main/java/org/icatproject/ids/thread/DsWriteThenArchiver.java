package org.icatproject.ids.thread;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.DfInfoImpl;
import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Copies datasets across storages (fast to slow) and subsequently
 * removes the files on the fast storage
 */
public class DsWriteThenArchiver implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DsWriteThenArchiver.class);

	private static final int BUFSIZ = 1024;

	private DsInfo dsInfo;

	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private FiniteStateMachine fsm;
	private Path datasetCache;
	private Path markerDir;

	private IcatReader reader;

	private ZipMapperInterface zipMapper;

	public DsWriteThenArchiver(DsInfo dsInfo, PropertyHandler propertyHandler,
			FiniteStateMachine fsm, IcatReader reader) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		this.zipMapper = propertyHandler.getZipMapper();
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
		markerDir = propertyHandler.getCacheDir().resolve("marker");
		this.reader = reader;
	}

	@Override
	public void run() {

		try {

			if (!mainStorageInterface.exists(dsInfo)) {
				logger.info("No files present for " + dsInfo + " - archive deleted");
				archiveStorageInterface.delete(dsInfo);
			} else {
				Path datasetCachePath = Files.createTempFile(datasetCache, null, null);
				List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile",
						dsInfo.getDsId())).getDatafiles();
				ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(datasetCachePath,
						StandardOpenOption.CREATE));
				zos.setLevel(0);
				for (Datafile datafile : datafiles) {
					String entryName = zipMapper
							.getFullEntryName(
									dsInfo,
									new DfInfoImpl(datafile.getId(), datafile.getName(), datafile
											.getLocation(), datafile.getCreateId(), datafile
											.getModId(), 0L));
					zos.putNextEntry(new ZipEntry(entryName));
					InputStream is = mainStorageInterface.get(datafile.getLocation(),
							datafile.getCreateId(), datafile.getModId());
					int bytesRead = 0;
					byte[] buffer = new byte[BUFSIZ];
					while ((bytesRead = is.read(buffer)) > 0) {
						zos.write(buffer, 0, bytesRead);
					}
					zos.closeEntry();
					is.close();
				}
				zos.close();

				InputStream is = Files.newInputStream(datasetCachePath);
				archiveStorageInterface.put(dsInfo, is);
				Files.delete(datasetCachePath);
				is.close();
			}
			Path marker = markerDir.resolve(Long.toString(dsInfo.getDsId()));
			Files.deleteIfExists(marker);
			logger.debug("Removed marker " + marker);
			mainStorageInterface.delete(dsInfo);
			logger.debug("Write then archive of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Write then archive of " + dsInfo + " failed due to " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}
