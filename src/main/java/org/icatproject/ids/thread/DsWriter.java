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
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.IdsBean;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copies dataset from main to archive
 */
public class DsWriter implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DsWriter.class);
	private static final int BUFSIZ = 1024;
	private DsInfo dsInfo;

	private FiniteStateMachine fsm;
	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private Path datasetCache;
	private Path markerDir;
	private IcatReader reader;
	private ZipMapperInterface zipMapper;

	public DsWriter(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm,
			IcatReader reader) {
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
			Path datasetCachePath = Files.createTempFile(datasetCache, null, null);

			if (!mainStorageInterface.exists(dsInfo)) {
				logger.info("No files present for " + dsInfo + " - archive deleted");
				mainStorageInterface.delete(dsInfo);
				archiveStorageInterface.delete(dsInfo);
			} else {
				logger.debug("Creating " + datasetCachePath);
				List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile",
						dsInfo.getDsId())).getDatafiles();

				ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(datasetCachePath,
						StandardOpenOption.CREATE));
				for (Datafile datafile : datafiles) {
					String location = IdsBean.getLocation(datafile);
					zos.putNextEntry(new ZipEntry(zipMapper.getFullEntryName(
							dsInfo,
							new DfInfoImpl(datafile.getId(), datafile.getName(), location, datafile
									.getCreateId(), datafile.getModId(), 0L))));
					InputStream is = mainStorageInterface.get(location, datafile.getCreateId(),
							datafile.getModId());
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
			logger.debug("Write of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Write of " + dsInfo + " failed due to " + e.getClass() + " "
					+ e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}
