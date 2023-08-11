package org.icatproject.ids;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.EntityBaseBean;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@Startup
public class FileChecker {

	@EJB
	IcatReader reader;

	private Long maxId;

	private final static String ls = System.getProperty("line.separator");

	private class CrcAndLength {

		private Long fileSize;

		private String checksum;

		private CrcAndLength(Datafile df) {
			this.checksum = df.getChecksum();
			this.fileSize = df.getFileSize();
		}

	}

	private class CheckOneFile implements Runnable {

		private EntityBaseBean eb;

		private CheckOneFile(EntityBaseBean eb) {
			this.eb = eb;
		}

		@Override
		public void run() {

			if (twoLevel) {
				Dataset ds = (Dataset) eb;
				logger.info("Checking Dataset " + ds.getId() + " (" + ds.getName() + ")");
				Map<String, CrcAndLength> crcAndLength = new HashMap<>();
				for (Datafile df : ds.getDatafiles()) {
					if (df.getLocation() != null) {
						crcAndLength.put(df.getName(), new CrcAndLength(df));
					}
				}
				if (!crcAndLength.isEmpty()) {
					String dfName = null;

					DsInfo dsInfo;
					try {
						dsInfo = new DsInfoImpl(ds);
					} catch (InsufficientPrivilegesException e) {
						report(ds, dfName, "Reports: " + e.getClass().getSimpleName() + " " + e.getMessage());
						return;
					}
					Path tPath = null;
					try {
						tPath = Files.createTempFile(null, null);
						archiveStorage.get(dsInfo, tPath);
						try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(tPath))) {
							ZipEntry ze = zis.getNextEntry();
							while (ze != null) {
								dfName = zipMapper.getFileName(ze.getName());
								CRC32 crc = new CRC32();
								byte[] bytes = new byte[1024];
								int length;
								long n = 0;
								while ((length = zis.read(bytes)) >= 0) {
									crc.update(bytes, 0, length);
									n += length;
								}

								CrcAndLength cl = crcAndLength.get(dfName);
								if (cl == null) {
									report(ds, dfName, "not found in map");
								} else if (cl.fileSize == null) {
									report(ds, dfName, "file size null");
								} else if (cl.fileSize != n) {
									report(ds, dfName, "file size wrong");
								} else if (cl.checksum == null) {
									report(ds, dfName, "checksum null");
								} else if (!cl.checksum.equals(Long.toHexString(crc.getValue()))) {
									report(ds, dfName, "checksum wrong");
								}

								crcAndLength.remove(dfName);
								ze = zis.getNextEntry();
							}
							if (!crcAndLength.isEmpty()) {
								report(ds, null, "zip file incomplete");
							}
						}
					} catch (IOException e) {
						report(ds, dfName,
								e.getClass().getName() + (e.getMessage() != null ? " " + e.getMessage() : ""));
					} catch (Throwable e) {
						e.printStackTrace();
						logger.error("Throwable " + e.getClass() + " " + e.getMessage());
					} finally {
						if (tPath != null) {
							try {
								Files.deleteIfExists(tPath);
							} catch (IOException e) {
								// Ignore
							}
						}
					}
				}
			} else {
				Datafile df = (Datafile) eb;
				logger.debug("Checking Datafile " + df.getId() + " (" + df.getName() + ")");
				String location;
				try {
					location = IdsBean.getLocation(df.getId(), df.getLocation());
				} catch (InsufficientPrivilegesException | InternalException e) {
					report(df, e.getClass() + " " + e.getMessage());
					return;
				}

				try (InputStream is = mainStorage.get(location, df.getCreateId(), df.getModId())) {
					CRC32 crc = new CRC32();
					byte[] bytes = new byte[1024];
					int length;
					long n = 0;
					while ((length = is.read(bytes)) >= 0) {
						crc.update(bytes, 0, length);
						n += length;
					}
					is.close();
					if (df.getFileSize() == null) {
						report(df, "file size null");
					} else if (df.getFileSize() != n) {
						report(df, "file size wrong");
					} else if (df.getChecksum() == null) {
						report(df, "checksum null");
					} else if (!df.getChecksum().equals(Long.toHexString(crc.getValue()))) {
						report(df, "checksum wrong");
					}
				} catch (IOException e) {
					report(df, e.getClass() + " " + e.getMessage());
				} catch (Throwable e) {
					logger.error("Throwable " + e.getClass() + " " + e.getMessage());
				}

			}

		}

		private void report(Datafile df, String emsg) {
			String msg = "Datafile " + df.getId() + " (" + df.getName() + ") ";
			msg += emsg;
			logger.info(msg);
			DateFormat dft = DateFormat.getDateTimeInstance();
			try {
				Files.write(filesCheckErrorLog, (dft.format(new Date()) + ": " + msg + ls).getBytes(),
						StandardOpenOption.APPEND, StandardOpenOption.CREATE);
			} catch (IOException e) {
				logger.error("Unable to write FileChecker log file " + e.getClass() + " " + e.getMessage());
			}
		}

		private void report(Dataset ds, String dfName, String emsg) {
			String msg = "Dataset " + ds.getId() + " (" + ds.getName() + ") ";
			if (dfName != null) {
				msg = msg + "datafile " + dfName + " ";
			}
			msg += emsg;
			logger.info(msg);
			DateFormat dft = DateFormat.getDateTimeInstance();
			try {
				Files.write(filesCheckErrorLog, (dft.format(new Date()) + ": " + msg + ls).getBytes(),
						StandardOpenOption.APPEND, StandardOpenOption.CREATE);
			} catch (IOException e) {
				logger.error("Unable to write FileChecker log file " + e.getClass() + " " + e.getMessage());
			}
		}
	}

	public class Action extends TimerTask {

		@Override
		public void run() {

			try {
				String query;
				if (twoLevel) {
					if (maxId != null) {
						query = "SELECT ds FROM Dataset ds WHERE ds.id > " + maxId
								+ " ORDER BY ds.id INCLUDE ds.datafiles, ds.investigation.facility LIMIT 0, "
								+ filesCheckParallelCount;
					} else {
						query = "SELECT ds FROM Dataset ds ORDER BY ds.id INCLUDE ds.datafiles, ds.investigation.facility LIMIT 0, "
								+ filesCheckParallelCount;
					}
				} else {
					if (maxId != null) {
						query = "SELECT df FROM Datafile df WHERE df.id > " + maxId + " AND df.location IS NOT NULL ORDER BY df.id LIMIT 0, "
								+ filesCheckParallelCount;
					} else {
						query = "SELECT df FROM Datafile df WHERE df.location IS NOT NULL ORDER BY df.id LIMIT 0, " + filesCheckParallelCount;
					}
				}
				List<Object> os = reader.search(query);

				logger.debug(query + " returned " + os.size() + " results");
				if (os.isEmpty()) {
					maxId = null;
				} else {

					List<Thread> threads = new ArrayList<Thread>(os.size());
					for (Object o : os) {
						EntityBaseBean eb = (EntityBaseBean) o;
						Thread worker = new Thread(new CheckOneFile(eb));
						worker.start();
						threads.add(worker);
						maxId = eb.getId();
					}
					for (Thread thread : threads) {
						try {
							thread.join();
						} catch (InterruptedException e) {
							logger.info("Thread interrupted");
							return;
						}
					}
				}

				try {
					if (maxId != null) {
						Files.write(filesCheckLastIdFile, Long.toString(maxId).getBytes());
					} else {
						Files.deleteIfExists(filesCheckLastIdFile);
					}
				} catch (IOException e) {
					logger.error("Unable to write FileChecker last id file " + e.getClass() + " " + e.getMessage());
				}

			} catch (IcatException_Exception e) {
				logger.error(e.getFaultInfo().getType() + " " + e.getMessage());
			} catch (Throwable e) {
				logger.error("Throwable " + e.getClass() + " " + e.getMessage());
			} finally {
				timer.schedule(new Action(), filesCheckGapMillis);
			}
		}

	}

	private final static Logger logger = LoggerFactory.getLogger(FileChecker.class);

	private Timer timer = new Timer();

	private int filesCheckParallelCount;
	private long filesCheckGapMillis;
	private Path filesCheckLastIdFile;
	private Path filesCheckErrorLog;

	private boolean twoLevel;

	private MainStorageInterface mainStorage;

	private ArchiveStorageInterface archiveStorage;

	private ZipMapperInterface zipMapper;

	private PropertyHandler propertyHandler;

	@PostConstruct
	public void init() {

		propertyHandler = PropertyHandler.getInstance();
		StorageUnit storageUnit = propertyHandler.getStorageUnit();
		filesCheckParallelCount = propertyHandler.getFilesCheckParallelCount();
		if (filesCheckParallelCount > 0) {
			logger.warn("The FileChecker is deprecated and slated for removal in ids.server 3.0");
			if (storageUnit == null || storageUnit == StorageUnit.DATASET) {
				filesCheckGapMillis = propertyHandler.getFilesCheckGapMillis();
				filesCheckLastIdFile = propertyHandler.getFilesCheckLastIdFile();
				filesCheckErrorLog = propertyHandler.getFilesCheckErrorLog();
				mainStorage = propertyHandler.getMainStorage();
				archiveStorage = propertyHandler.getArchiveStorage();
				twoLevel = archiveStorage != null;
				zipMapper = propertyHandler.getZipMapper();

				maxId = null;
				if (Files.exists(filesCheckLastIdFile)) {
					try {
						maxId = Long.parseLong(Files.readAllLines(filesCheckLastIdFile, StandardCharsets.UTF_8).get(0));
					} catch (Exception e) {
						logger.error("Unable to read FileChecker last id file " + e.getClass() + " " + e.getMessage());
					}
				}

				timer.schedule(new Action(), filesCheckGapMillis);

				logger.info("FileChecker started with maxId: " + maxId);
			} else {
				logger.info("FileChecker not supported for storageUnit " + storageUnit);
			}
		} else {
			logger.info("FileChecker startup not requested");
		}

	}

	@PreDestroy
	public void exit() {
		timer.cancel();
		logger.info("FileChecker stopped");
	}

}
