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
				cleanPreparedDir(preparedDir, preparedCount);

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
						logger.debug("Main storage is " + (float) used * 100 / startArchivingLevel
								+ "% full");
						List<Long> investigations = mainStorage.getInvestigations();
						outer: while (true) {
							for (Long invId : investigations) {

								for (Long dsId : mainStorage.getDatasets(invId)) {

									try {
										String query = "SELECT sum(df.fileSize) FROM Datafile df WHERE df.dataset.id = "
												+ dsId;
										long size = (Long) reader.search(query).get(0);
										Dataset ds = (Dataset) reader.get(
												"Dataset ds INCLUDE ds.investigation.facility",
												dsId);
										DsInfoImpl dsInfoImpl = new DsInfoImpl(ds);
										logger.debug("Requesting archive of " + dsInfoImpl
												+ " to recover " + size + " bytes of main storage");
										fsm.queue(dsInfoImpl, DeferredOp.ARCHIVE);
										used -= size;
										if (used < stopArchivingLevel) {
											logger.debug("After archiving main storage will be "
													+ (float) used * 100 / startArchivingLevel
													+ "% full");
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

	private final static Logger logger = LoggerFactory.getLogger(Tidier.class);;

	static void cleanPreparedDir(Path preparedDir, int preparedCount) throws IOException {

		Map<Long, Path> dateMap = new HashMap<>();
		File[] files = preparedDir.toFile().listFiles();
		int ndel = files.length - preparedCount;
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
				logger.debug("Deleted " + path);
				if (ndel == 0) {
					break;
				}
			}
		}
	}

	@EJB
	private FiniteStateMachine fsm;

	private Path linkDir;
	private long linkLifetimeMillis;
	private MainStorageInterface mainStorage;

	private Path preparedDir;

	@EJB
	IcatReader reader;

	private long sizeCheckIntervalMillis;
	private long startArchivingLevel;
	private long stopArchivingLevel;
	private Timer timer = new Timer();
	private boolean twoLevel;
	private int preparedCount;

	@PreDestroy
	public void exit() {
		timer.cancel();
		logger.info("Tidier stopped");
	}

	@PostConstruct
	public void init() {
		try {
			PropertyHandler propertyHandler = PropertyHandler.getInstance();
			sizeCheckIntervalMillis = propertyHandler.getSizeCheckIntervalMillis();
			preparedCount = propertyHandler.getPreparedCount();
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
