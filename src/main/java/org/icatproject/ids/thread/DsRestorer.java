package org.icatproject.ids.thread;

import java.io.IOException;
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
import org.icatproject.ids.IdsBean;
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
public class DsRestorer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(DsRestorer.class);

	private DsInfo dsInfo;

	private MainStorageInterface mainStorageInterface;
	private ArchiveStorageInterface archiveStorageInterface;
	private FiniteStateMachine fsm;

	private Path datasetCache;

	private IcatReader reader;

	private ZipMapperInterface zipMapper;

	public DsRestorer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm, IcatReader reader) {
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
			/*
			 * This code avoids unnecessary calls to restore files. It will not
			 * generally do anything as pointless restores are normally filtered
			 * out earlier.
			 */
			try {
				if (mainStorageInterface.exists(dsInfo)) {
					return;
				}
			} catch (IOException e) {
				logger.error("Check on existence of {} failed with {} {}", dsInfo.getDsLocation(), e.getClass(),
						e.getMessage());
			}

			long size = 0;
			int n = 0;
			List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile", dsInfo.getDsId()))
					.getDatafiles();
			Map<String, String> nameToLocalMap = new HashMap<>(datafiles.size());
			for (Datafile datafile : datafiles) {
				nameToLocalMap.put(datafile.getName(), IdsBean.getLocation(datafile.getId(), datafile.getLocation()));
				size += datafile.getFileSize();
				n++;
			}

			logger.debug("Restoring dataset " + dsInfo.getInvId() + "/" + dsInfo.getDsId() + " with " + n
					+ " files of total size " + size);

			// Get the file into the dataset cache
			Path datasetCachePath = Files.createTempFile(datasetCache, null, null);
			archiveStorageInterface.get(dsInfo, datasetCachePath);

			// Now split file and store it locally
			logger.debug("Unpacking dataset " + dsInfo.getInvId() + "/" + dsInfo.getDsId() + " with " + n
					+ " files of total size " + size);
			ZipInputStream zis = new ZipInputStream(Files.newInputStream(datasetCachePath));
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String dfName = zipMapper.getFileName(ze.getName());
				String location = nameToLocalMap.get(dfName);
				if (location == null) {
					logger.error("Unable to store " + dfName + " into " + dsInfo + " as no location found");
				} else {
					mainStorageInterface.put(zis, location);
				}
				ze = zis.getNextEntry();
			}
			zis.close();
			Files.delete(datasetCachePath);
			fsm.recordSuccess(dsInfo.getDsId());
			logger.debug("Restore of " + dsInfo + " completed");
		} catch (Exception e) {
			fsm.recordFailure(dsInfo.getDsId());
			logger.error("Restore of " + dsInfo + " failed due to " + e.getClass() + " " + e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}