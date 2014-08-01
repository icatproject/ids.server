package org.icatproject.ids;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class Tidier {

	@EJB
	IcatReader reader;

	@EJB
	private FiniteStateMachine fsm;

	public class TreeSizeVisitor extends SimpleFileVisitor<Path> {

		private long size;

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			size += Files.size(file);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
			if (e == null) {
				size += Files.size(dir);
				return FileVisitResult.CONTINUE;
			} else {
				// directory iteration failed
				throw e;
			}
		}

		public long getSize() {
			return size;
		}

	}

	public class Action extends TimerTask {

		@Override
		public void run() {
			try {
				clean(preparedDir, preparedCacheSizeBytes);
				if (datasetCacheSizeBytes != 0) {
					clean(datasetDir, datasetCacheSizeBytes);
				}
				if (twoLevel) {
					long free = mainStorage.getUsableSpace();
					if (free < minFreeSpace) {
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
										fsm.queue(new DsInfoImpl(ds), DeferredOp.ARCHIVE);
										long size = (Long) reader.search(query).get(0);
										free -= size;
										if (free < maxFreeSpace) {
											break outer;
										}
									} catch (InternalException | IcatException_Exception e) {
										// Log it and carry on
										logger.error(e.getClass() + " " + e.getMessage());
									}
								}
							}
						}
					}
				}
			} catch (IOException e) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				e.printStackTrace(new PrintStream(baos));
				logger.error(e.getClass() + " " + e.getMessage() + baos);
			} finally {
				timer.schedule(new Action(), sizeCheckIntervalMillis);
			}
		}

		private void clean(Path dir, long bytes) throws IOException {
			Map<Path, Long> size = new HashMap<>();
			Map<Long, Path> date = new HashMap<>();
			long totalSize = 0;
			for (File file : dir.toFile().listFiles()) {
				Path path = file.toPath();
				TreeSizeVisitor sizer = new TreeSizeVisitor();
				Files.walkFileTree(path, sizer);
				long thisSize = sizer.getSize();
				size.put(path, thisSize);
				date.put(file.lastModified(), path);
				totalSize += thisSize;
			}
			if (totalSize != 0) {
				logger.debug(dir + " is " + (float) totalSize * 100 / bytes + "% full");
				if (totalSize > bytes) {
					List<Long> dates = new ArrayList<>(date.keySet());
					Collections.sort(dates);
					while (totalSize > bytes) {
						Path path = date.get(dates.get(0));
						Files.walkFileTree(path, deleter);
						totalSize -= size.get(path);
					}
					logger.debug(dir + " is now " + (float) totalSize * 100 / bytes + "% full");
				}
			}
		}
	}

	private long preparedCacheSizeBytes;
	private long sizeCheckIntervalMillis;
	private long datasetCacheSizeBytes;
	private Path preparedDir;
	private Path datasetDir;

	private final static Logger logger = LoggerFactory.getLogger(Tidier.class);

	private Timer timer = new Timer();
	private TreeDeleteVisitor deleter;
	private MainStorageInterface mainStorage;
	private boolean twoLevel;
	private long minFreeSpace;
	private long maxFreeSpace;

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
			mainStorage = propertyHandler.getMainStorage();
			twoLevel = propertyHandler.getArchiveStorage() != null;
			if (twoLevel) {
				mainStorage = propertyHandler.getMainStorage();
				minFreeSpace = propertyHandler.getMinFreeSpace();
				maxFreeSpace = propertyHandler.getMaxFreeSpace();
			}
			timer.schedule(new Action(), sizeCheckIntervalMillis);
			deleter = new TreeDeleteVisitor();
			logger.info("Tidier started");
		} catch (IOException e) {
			throw new RuntimeException("Tidier reports " + e.getClass() + " " + e.getMessage());
		}

	}

	@PreDestroy
	public void exit() {
		timer.cancel();
		logger.info("Tidier stopped");
	}

}
