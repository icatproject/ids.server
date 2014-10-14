package org.icatproject.ids.thread;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.FiniteStateMachine;
import org.icatproject.ids.IcatReader;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
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

	private IcatReader reader;

	private ZipMapperInterface zipMapper;

	public Restorer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm,
			IcatReader reader) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		zipMapper = propertyHandler.getZipMapper();
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
		this.reader = reader;
	}

	@Override
	public void run() {
		try {
			long size = 0;
			int n = 0;
			List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile",
					dsInfo.getDsId())).getDatafiles();
			Map<String, String> nameToLocalMap = new HashMap<>(datafiles.size());
			for (Datafile datafile : datafiles) {
				nameToLocalMap.put(datafile.getName(), datafile.getLocation());
				size += datafile.getFileSize();
				n++;
			}

			logger.debug("Restoring dataset " + dsInfo.getInvId() + "/" + dsInfo.getDsId()
					+ " with " + n + " files of total size " + size);

			// Get the file into the dataset cache
			Path datasetCachePath = Files.createTempFile(datasetCache, null, null);
			archiveStorageInterface.get(dsInfo, datasetCachePath);

			// Now split file and store it locally
			logger.debug("Unpacking dataset " + dsInfo.getInvId() + "/" + dsInfo.getDsId()
					+ " with " + n + " files of total size " + size);
			ZipInputStream zis = new ZipInputStream(Files.newInputStream(datasetCachePath));
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String dfName = zipMapper.getFileName(ze.getName());
				String location = nameToLocalMap.get(dfName);
				if (location == null) {
					logger.error("Unable to store " + dfName + " into " + dsInfo
							+ " as no location found");
				} else {
					mainStorageInterface.put(zis, location);
				}
				ze = zis.getNextEntry();
			}
			zis.close();
			Files.delete(datasetCachePath);
			logger.debug("Restore of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Restore of " + dsInfo + " failed due to " + e.getClass() + " "
					+ e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}