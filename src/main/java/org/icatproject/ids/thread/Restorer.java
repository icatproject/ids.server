package org.icatproject.ids.thread;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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

	public Restorer(DsInfo dsInfo, PropertyHandler propertyHandler, FiniteStateMachine fsm,
			IcatReader reader) {
		this.dsInfo = dsInfo;
		this.fsm = fsm;
		mainStorageInterface = propertyHandler.getMainStorage();
		archiveStorageInterface = propertyHandler.getArchiveStorage();
		datasetCache = propertyHandler.getCacheDir().resolve("dataset");
		this.reader = reader;
	}

	@Override
	public void run() {
		try {
			// Get the file into the dataset cache
			Path dir = datasetCache.resolve(Long.toString(dsInfo.getInvId()));
			Files.createDirectories(dir);
			Path tPath = Files.createTempFile(dir, null, null);
			InputStream is = archiveStorageInterface.get(dsInfo);
			Files.copy(is, tPath, StandardCopyOption.REPLACE_EXISTING);
			is.close();
			Path path = dir.resolve(Long.toString(dsInfo.getDsId()));
			Files.move(tPath, path, StandardCopyOption.ATOMIC_MOVE,
					StandardCopyOption.REPLACE_EXISTING);

			List<Datafile> datafiles = ((Dataset) reader.get("Dataset INCLUDE Datafile",
					dsInfo.getDsId())).getDatafiles();
			Map<String, String> nameToLocaMap = new HashMap<>(datafiles.size());
			for (Datafile datafile : datafiles) {
				nameToLocaMap.put(datafile.getName(), datafile.getLocation());
			}
			Path dsPath = new File("ids/" + dsInfo.getFacilityName() + "/" + dsInfo.getInvName()
					+ "/" + dsInfo.getVisitId() + "/" + dsInfo.getDsName()).toPath();
			// Now split file and store it locally
			ZipInputStream zis = new ZipInputStream(Files.newInputStream(path));
			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
				String dfName = dsPath.relativize(new File(ze.getName()).toPath()).toString();
				String location = nameToLocaMap.get(dfName);
				if (location == null) {
					logger.error("Unable to store " + dfName + " into " + dsInfo
							+ " as no location found");
				} else {
					logger.debug("Storing " + dfName + " into " + dsInfo + " at " + location);
					mainStorageInterface.put(zis, location);
				}
				ze = zis.getNextEntry();
			}
			zis.close();
			logger.debug("Restore of " + dsInfo + " completed");
		} catch (Exception e) {
			logger.error("Restore of " + dsInfo + " failed due to " + e.getClass() + " "
					+ e.getMessage());
		} finally {
			fsm.removeFromChanging(dsInfo);
		}
	}
}