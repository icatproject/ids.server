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
import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class Tidier {

	private static final Logger logger = LoggerFactory.getLogger(Tidier.class);

	private MainStorageInterface mainStorage;
	private long sizeCheckIntervalMillis;
	private long startArchivingLevel;
	private long stopArchivingLevel;
	private Path preparedDir;
	private int preparedCount;
	private Path completedDir;
	private int completedCount;
	private Path failedFilesDir;
	private int failedFilesCount;

	private Timer timer = new Timer();

	@PostConstruct
	public void init() {
		try {
			PropertyHandler propertyHandler = PropertyHandler.getInstance();
			sizeCheckIntervalMillis = propertyHandler.getSizeCheckIntervalMillis();
			preparedCount = propertyHandler.getPreparedCount();
			completedCount = propertyHandler.getCompletedCount();
			failedFilesCount = propertyHandler.getFailedFilesCount();
			preparedDir = propertyHandler.getCacheDir().resolve(Constants.PREPARED_DIR_NAME);
			Files.createDirectories(preparedDir);
			completedDir = propertyHandler.getCacheDir().resolve(Constants.COMPLETED_DIR_NAME);
			Files.createDirectories(completedDir);
			failedFilesDir = propertyHandler.getCacheDir().resolve(Constants.FAILED_DIR_NAME);
			Files.createDirectories(failedFilesDir);
			mainStorage = propertyHandler.getMainStorage();
			startArchivingLevel = propertyHandler.getStartArchivingLevel();
			stopArchivingLevel = propertyHandler.getStopArchivingLevel();

			timer.schedule(new Action(), sizeCheckIntervalMillis);

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

	public class Action extends TimerTask {

		@Override
		public void run() {
			// Really only needs doing once because Timer has one thread for all tasks
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
			try {
				// tidy up the cache sub-directories
				deleteOldestFilesFromDir(preparedDir, preparedCount);
				deleteOldestFilesFromDir(completedDir, completedCount);
				deleteOldestFilesFromDir(failedFilesDir, failedFilesCount);

				List<DfInfo> dfInfos = mainStorage.getDatafilesToArchive(stopArchivingLevel, startArchivingLevel);
				// MainSDStorage logs size of cache area and how many files it
				// has found to archive so no need to log that here
				for (DfInfo dfInfo : dfInfos) {
					logger.debug("Deleting from main storage: {}", dfInfo.getDfLocation());
					// note that the createId and modId parameters are
					// not required by the MainSDStorage implementation
					mainStorage.delete(dfInfo.getDfLocation(), null, null);
				}
			} catch (IOException e) {
				logger.error("{} {}", e.getClass(), e.getMessage());
			} finally {
				timer.schedule(new Action(), sizeCheckIntervalMillis);
			}
		}

	}

	static void deleteOldestFilesFromDir(Path dirToClean, int maxFilesToKeep) throws IOException {
		Map<Long, Path> dateMap = new HashMap<>();
		File[] files = dirToClean.toFile().listFiles();
		int ndel = files.length - maxFilesToKeep;
		if (ndel > 0) {
			for (File file : files) {
				Path path = file.toPath();
				dateMap.put(file.lastModified(), path);
			}
			List<Long> dates = new ArrayList<>(dateMap.keySet());
			Collections.sort(dates);
			for (Long adate : dates) {
				Path path = dateMap.get(adate);
				Files.delete(path);
				ndel--;
				logger.debug("Deleted {}", path);
				if (ndel == 0) {
					break;
				}
			}
		}
	}

}
