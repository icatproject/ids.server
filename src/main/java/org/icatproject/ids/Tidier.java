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

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class Tidier {

	public class Action extends TimerTask {

		@Override
		public void run() {
			// Really only needs doing once because Timer has one thread for all
			// tasks
			Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
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
					if (storageUnit == StorageUnit.DATASET) {
						List<DsInfo> dsInfos = mainStorage.getDatasetsToArchive(stopArchivingLevel,
								startArchivingLevel);
						for (DsInfo dsInfo : dsInfos) {
							StringBuilder sb = new StringBuilder(
									"SELECT ds FROM Dataset ds, ds.investigation inv, inv.facility fac WHERE");
							boolean andNeeded = false;
							andNeeded = addNumericConstraint(sb, "ds.id", dsInfo.getDsId(), andNeeded);
							andNeeded = addStringConstraint(sb, "ds.location", dsInfo.getDsLocation(), andNeeded);
							andNeeded = addStringConstraint(sb, "ds.name", dsInfo.getDsName(), andNeeded);

							andNeeded = addNumericConstraint(sb, "inv.id", dsInfo.getInvId(), andNeeded);
							andNeeded = addStringConstraint(sb, "inv.name", dsInfo.getInvName(), andNeeded);
							andNeeded = addStringConstraint(sb, "inv.visitId", dsInfo.getVisitId(), andNeeded);

							andNeeded = addStringConstraint(sb, "fac.name", dsInfo.getFacilityName(), andNeeded);
							andNeeded = addNumericConstraint(sb, "fac.id", dsInfo.getFacilityId(), andNeeded);

							sb.append(" INCLUDE ds.investigation.facility");
							try {
								int low = 0;
								while (true) {
									String query = sb.toString() + " LIMIT " + low + "," + tidyBlockSize;
									List<Object> os = reader.search(query);
									logger.debug(query + " returns " + os.size() + " datasets");
									for (Object o : os) {
										DsInfoImpl dsInfoImpl = new DsInfoImpl((Dataset) o);
										logger.debug(
												"Requesting archive of " + dsInfoImpl + " to recover main storage");
										fsm.queue(dsInfoImpl, DeferredOp.ARCHIVE);
									}
									if (os.size() < tidyBlockSize) {
										break;
									}
									low += tidyBlockSize;
								}
							} catch (InternalException | IcatException_Exception | InsufficientPrivilegesException e) {
								// Log it and carry on
								logger.error(e.getClass() + " " + e.getMessage());
							}
						}

					} else if (storageUnit == StorageUnit.DATAFILE) {
						List<DfInfo> dfInfos = mainStorage.getDatafilesToArchive(stopArchivingLevel,
								startArchivingLevel);
						for (DfInfo dfInfo : dfInfos) {
							StringBuilder sb = new StringBuilder("SELECT df FROM Datafile df WHERE");
							boolean andNeeded = false;

							andNeeded = addNumericConstraint(sb, "df.id", dfInfo.getDfId(), andNeeded);
							andNeeded = addStringConstraint(sb, "df.createId", dfInfo.getCreateId(), andNeeded);
							andNeeded = addStringConstraint(sb, "df.modId", dfInfo.getModId(), andNeeded);
							if (key != null) {
								if (dfInfo.getDfLocation() != null) {
									if (andNeeded) {
										sb.append(" AND ");
									} else {
										sb.append(" ");
										andNeeded = true;
									}
									sb.append("df.location" + " LIKE '" + dfInfo.getDfLocation() + " %'");
								}
							} else {
								andNeeded = addStringConstraint(sb, "df.location",

										dfInfo.getDfLocation(), andNeeded);
							}
							andNeeded = addStringConstraint(sb, "df.name", dfInfo.getDfName(), andNeeded);

							sb.append(" INCLUDE df.dataset");
							try {
								int low = 0;
								while (true) {
									String query = sb.toString() + " LIMIT " + low + "," + tidyBlockSize;
									List<Object> os = reader.search(query);
									logger.debug(query + " returns " + os.size() + " datafiles");
									for (Object o : os) {
										Datafile df = (Datafile) o;
										DfInfoImpl dfInfoImpl = new DfInfoImpl(df.getId(), df.getName(),

												IdsBean.getLocation(df.getId(), df.getLocation()), df.getCreateId(),
												df.getModId(), df.getDataset().getId());


										logger.debug(
												"Requesting archive of " + dfInfoImpl + " to recover main storage");
										fsm.queue(dfInfoImpl, DeferredOp.ARCHIVE);
									}
									if (os.size() < tidyBlockSize) {
										break;
									}
									low += tidyBlockSize;
								}
							} catch (InternalException | IcatException_Exception | InsufficientPrivilegesException e) {
								// Log it and carry on
								logger.error(e.getClass() + " " + e.getMessage());
							}
						}
					}
				}
			} catch (Throwable e) {
				logger.error(e.getClass() + " " + e.getMessage());
			} finally {
				timer.schedule(new Action(), sizeCheckIntervalMillis);
			}
		}

	}

	private final static Logger logger = LoggerFactory.getLogger(Tidier.class);

	static boolean addStringConstraint(StringBuilder sb, String var, String value, boolean andNeeded) {
		if (value != null) {
			if (andNeeded) {
				sb.append(" AND ");
			} else {
				sb.append(" ");
				andNeeded = true;
			}
			sb.append(var + " = '" + value.replaceAll("'", "''") + "'");
		}
		return andNeeded;
	}

	static boolean addNumericConstraint(StringBuilder sb, String var, Long value, boolean andNeeded) {
		if (value != null) {
			if (andNeeded) {
				sb.append(" AND ");
			} else {
				sb.append(" ");
				andNeeded = true;
			}
			sb.append(var + " = " + value);
		}
		return andNeeded;
	}

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

	private StorageUnit storageUnit;

	private int tidyBlockSize;

	private String key;

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
			preparedDir = propertyHandler.getCacheDir().resolve(Constants.PREPARED_DIR_NAME);
			Files.createDirectories(preparedDir);
			linkDir = propertyHandler.getCacheDir().resolve("link");
			Files.createDirectories(linkDir);
			linkLifetimeMillis = propertyHandler.getLinkLifetimeMillis();
			mainStorage = propertyHandler.getMainStorage();
			twoLevel = propertyHandler.getArchiveStorage() != null;
			key = propertyHandler.getKey();
			if (twoLevel) {
				mainStorage = propertyHandler.getMainStorage();
				startArchivingLevel = propertyHandler.getStartArchivingLevel();
				stopArchivingLevel = propertyHandler.getStopArchivingLevel();
				storageUnit = propertyHandler.getStorageUnit();
				tidyBlockSize = propertyHandler.getTidyBlockSize();
			}
			timer.schedule(new Action(), sizeCheckIntervalMillis);

			logger.info("Tidier started");
		} catch (IOException e) {
			throw new RuntimeException("Tidier reports " + e.getClass() + " " + e.getMessage());
		}

	}

}
