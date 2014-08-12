package org.icatproject.ids;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.icatproject.Dataset;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class Tidier {

	public class Action extends TimerTask {

		@Override
		public void run() {
			try {
				cleanPreparedDir(preparedDir, preparedCacheSizeBytes);

				if (datasetCacheSizeBytes != 0) {
					cleanDatasetCache(datasetDir, datasetCacheSizeBytes);
				}
				if (linkLifetimeMillis > 0) {
					long deleteMillis = System.currentTimeMillis() - linkLifetimeMillis;
					int n = 0;
					for (File f : linkDir.toFile().listFiles()) {
						Path p = f.toPath();

						if (Files.getLastModifiedTime(p).toMillis() < deleteMillis) {
							try {
								Files.delete(p);
								n++;
							} catch (Exception e) {
								logger.error(e.getClass() + " " + e.getMessage());
							}
						}
					}
					if (n > 0) {
						logger.debug("Deleted " + n + " links from " + linkDir);
					}
				}
				if (twoLevel) {
					long used = mainStorage.getUsedSpace();
					if (used > startArchivingLevel) {
						List<Long> investigations = mainStorage.getInvestigations();
						outer: while (true) {
							for (Long invId : investigations) {
								for (Long dsId : mainStorage.getDatasets(invId)) {
									try {
										String query = "SELECT sum(filesize) FROM Datafile df WHERE df.dataset_id = "
												+ dsId;
										Dataset ds = (Dataset) reader.get(
												"Dataset ds INCLUDE ds.investigation.facility",
												dsId);
										DsInfoImpl dsInfoImpl = new DsInfoImpl(ds);
										logger.debug("Requesting archive of " + dsInfoImpl
												+ " to recover space");
										fsm.queue(dsInfoImpl, DeferredOp.ARCHIVE);
										long size = (Long) reader.search(query).get(0);
										used -= size;
										if (used < stopArchivingLevel) {
											break outer;
										}
									} catch (InternalException | IcatException_Exception
											| InsufficientPrivilegesException e) {
										// Log it and carry on
										logger.error(e.getClass() + " " + e.getMessage());
									}
								}
							}
						}
					}
				}
			} catch (IOException e) {
				logger.error(e.getClass() + " " + e.getMessage());
			} finally {
				timer.schedule(new Action(), sizeCheckIntervalMillis);
			}
		}

	}

	private static final long DAY = 24 * 3600 * 1000;
	private final static Logger logger = LoggerFactory.getLogger(Tidier.class);;

	private static void clean(Path dir, Map<Long, Path> date, Map<Path, Long> size, long totalSize,
			long bytes) throws IOException {
		if (totalSize > bytes) {
			logger.debug(dir + " is " + (float) totalSize * 100 / bytes + "% full");
			List<Long> dates = new ArrayList<>(date.keySet());
			Collections.sort(dates);
			long now = System.currentTimeMillis();
			for (Long adate : dates) {
				Path path = date.get(adate);
				String pf = path.getFileName().toString();
				if (now - adate < DAY && (pf.startsWith("tmp.") || pf.endsWith(".tmp"))) {
					logger.debug("Left recenly modified temporary file " + path + " of size "
							+ size.get(path) + " bytes");
				} else {
					if (Files.isDirectory(path)) {
						for (File f : path.toFile().listFiles()) {
							Files.delete(f.toPath());
						}
					}
					Files.delete(path);
					logger.debug("Deleted " + path + " to reclaim " + size.get(path) + " bytes");
					totalSize -= size.get(path);
					if (totalSize <= bytes) {
						break;
					}
				}
			}
			logger.debug(dir + " is now " + (float) totalSize * 100 / bytes + "% full");
		}
	}

	static void cleanDatasetCache(Path datasetDir, long datasetCacheSizeBytes) throws IOException {
		Map<Path, Long> sizeMap = new HashMap<>();
		Map<Long, Path> dateMap = new HashMap<>();
		long totalSize = 0;
		for (File inv : datasetDir.toFile().listFiles()) {
			for (File dsFile : inv.listFiles()) {
				Path path = dsFile.toPath();
				long thisSize = Files.size(path);
				sizeMap.put(path, thisSize);
				dateMap.put(dsFile.lastModified(), path);
				totalSize += thisSize;
			}
		}
		clean(datasetDir, dateMap, sizeMap, totalSize, datasetCacheSizeBytes);
		for (File inv : datasetDir.toFile().listFiles()) {
			Path invPath = inv.toPath();
			try {
				Files.delete(invPath);
				logger.debug("Deleted complete investigation directory from dataset cache "
						+ invPath);
			} catch (Exception e) {
				// Ignore - probably not empty
			}
		}

	}

	static void cleanPreparedDir(Path preparedDir, long preparedCacheSizeBytes) throws IOException {
		Map<Path, Long> sizeMap = new HashMap<>();
		Map<Long, Path> dateMap = new HashMap<>();
		long totalSize = 0;
		for (File file : preparedDir.toFile().listFiles()) {
			Path path = file.toPath();
			long thisSize = 0;
			if (Files.isDirectory(path)) {
				for (File notZipFile : file.listFiles()) {
					thisSize += Files.size(notZipFile.toPath());
				}
			}
			thisSize += Files.size(path);
			sizeMap.put(path, thisSize);
			dateMap.put(file.lastModified(), path);
			totalSize += thisSize;
		}
		clean(preparedDir, dateMap, sizeMap, totalSize, preparedCacheSizeBytes);
	}

	private long datasetCacheSizeBytes;
	private Path datasetDir;

	@EJB
	private FiniteStateMachine fsm;

	private Path linkDir;
	private long linkLifetimeMillis;
	private MainStorageInterface mainStorage;
	private long preparedCacheSizeBytes;
	private Path preparedDir;

	@EJB
	IcatReader reader;

	private long sizeCheckIntervalMillis;
	private long startArchivingLevel;
	private long stopArchivingLevel;
	private Timer timer = new Timer();
	private boolean twoLevel;

	@PreDestroy
	public void exit() {
		timer.cancel();
		logger.info("Tidier stopped");
	}

	@PostConstruct
	public void init() {
		try {
			PropertyHandler propertyHandler = PropertyHandler.getInstance();
			preparedCacheSizeBytes = propertyHandler.getPreparedCacheSizeBytes();
			sizeCheckIntervalMillis = propertyHandler.getSizeCheckIntervalMillis();
			datasetCacheSizeBytes = propertyHandler.getDatasetCacheSizeBytes();
			datasetDir = propertyHandler.getCacheDir().resolve("dataset");
			Files.createDirectories(datasetDir);
			preparedDir = propertyHandler.getCacheDir().resolve("prepared");
			Files.createDirectories(preparedDir);
			linkDir = propertyHandler.getCacheDir().resolve("link");
			Files.createDirectories(linkDir);
			linkLifetimeMillis = propertyHandler.getlinkLifetimeMillis();
			mainStorage = propertyHandler.getMainStorage();
			twoLevel = propertyHandler.getArchiveStorage() != null;
			if (twoLevel) {
				mainStorage = propertyHandler.getMainStorage();
				startArchivingLevel = propertyHandler.getStartArchivingLevel();
				stopArchivingLevel = propertyHandler.getStopArchivingLevel();
			}
			timer.schedule(new Action(), sizeCheckIntervalMillis);

			logger.info("Tidier started");
		} catch (IOException e) {
			throw new RuntimeException("Tidier reports " + e.getClass() + " " + e.getMessage());
		}

	}

}
