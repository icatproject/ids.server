package org.icatproject.ids;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.datatype.DatatypeFactory;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.EntityBaseBean;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.Login.Credentials;
import org.icatproject.Login.Credentials.Entry;
import org.icatproject.ids.DataSelection.Returns;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.IdsException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
public class IdsBean {

	@EJB
	IcatReader reader;

	private static final int BUFSIZ = 2048;

	private static final String prefix = "<html><script type=\"text/javascript\">window.name='";
	private static final String suffix = "';</script></html>";
	private static String paddedPrefix;
	static {
		paddedPrefix = "<html><script type=\"text/javascript\">/*";
		for (int n = 1; n < 25; n++) {
			paddedPrefix += " *        \n";
		}
		paddedPrefix += "*/window.name='";
	}

	private static Boolean inited = false;

	private final static Logger logger = LoggerFactory.getLogger(IdsBean.class);

	/** matches standard UUID format of 8-4-4-4-12 hexadecimal digits */
	public static final Pattern uuidRegExp = Pattern
			.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

	public static void validateUUID(String thing, String id) throws BadRequestException {
		if (id == null || !uuidRegExp.matcher(id).matches())
			throw new BadRequestException("The " + thing + " parameter '" + id
					+ "' is not a valid UUID");
	}

	private DatatypeFactory datatypeFactory;

	@EJB
	private FiniteStateMachine fsm;

	private ICAT icat;

	private MainStorageInterface mainStorage;

	private Path preparedDir;

	private PropertyHandler propertyHandler;

	private boolean twoLevel;

	private Path datasetDir;

	private Path markerDir;

	private Set<String> rootUserNames;

	private boolean readOnly;

	private ZipMapperInterface zipMapper;

	private Path linkDir;

	private boolean linkEnabled;

	public void archive(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, InternalException, NotFoundException {

		// Log and validate
		logger.info("New webservice request: archive " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS);

		// Do it
		if (twoLevel) {
			Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos.values()) {
				fsm.queue(dsInfo, DeferredOp.ARCHIVE);
			}
		}
	}

	public void delete(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, InternalException, NotFoundException,
			DataNotOnlineException {

		logger.info("New webservice request: delete " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		if (readOnly) {
			throw new NotImplementedException(
					"This operation has been configured to be unavailable");
		}

		IdsBean.validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		DataNotOnlineException exc = null;

		Collection<DsInfo> dsInfos = dataSelection.getDsInfo().values();
		if (twoLevel) {
			Set<Long> emptyDatasets = dataSelection.getEmptyDatasets();
			try {
				for (DsInfo dsInfo : dsInfos) {
					if (!emptyDatasets.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
						fsm.queue(dsInfo, DeferredOp.RESTORE);
						exc = new DataNotOnlineException(
								"Before deleting a datafile, its dataset has to be restored, restoration requested automatically");
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		if (exc != null) {
			throw exc;
		}

		for (DsInfo dsInfo : dsInfos) {
			logger.debug("DS " + dsInfo.getDsId() + " " + dsInfo);
			if (fsm.isLocked(dsInfo.getDsId())) {
				throw new BadRequestException("Dataset " + dsInfo
						+ " (or a part of it) is currently being streamed to a user");
			}
		}

		/* Now delete from ICAT */
		List<EntityBaseBean> dfs = new ArrayList<>();
		for (DfInfoImpl dfInfo : dataSelection.getDfInfo()) {
			Datafile df = new Datafile();
			df.setId(dfInfo.getDfId());
			dfs.add(df);
		}
		try {
			icat.deleteMany(sessionId, dfs);
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();

			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		}

		try {
			for (DfInfoImpl dfInfo : dataSelection.getDfInfo()) {
				mainStorage.delete(dfInfo.getDfLocation());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (twoLevel) {
			for (DsInfo dsInfo : dsInfos) {
				fsm.queue(dsInfo, DeferredOp.WRITE);
			}
		}

	}

	static Prepared unpack(InputStream stream) throws InternalException {
		Prepared prepared = new Prepared();
		JsonObject pd;
		try (JsonReader jsonReader = Json.createReader(stream)) {
			pd = jsonReader.readObject();
		}
		prepared.zip = pd.getBoolean("zip");
		prepared.compress = pd.getBoolean("compress");
		Map<Long, DsInfo> dsInfos = new HashMap<>();
		Set<DfInfoImpl> dfInfos = new HashSet<>();
		Set<Long> emptyDatasets = new HashSet<>();

		for (JsonValue itemV : pd.getJsonArray("dfInfo")) {
			JsonObject item = (JsonObject) itemV;
			String dfLocation = item.isNull("dfLocation") ? null : item.getString("dfLocation");
			dfInfos.add(new DfInfoImpl(item.getJsonNumber("dfId").longValueExact(), item
					.getString("dfName"), dfLocation, item.getString("createId"), item
					.getString("modId"), item.getJsonNumber("dsId").longValueExact()));

		}
		prepared.dfInfos = dfInfos;

		for (JsonValue itemV : pd.getJsonArray("dsInfo")) {
			JsonObject item = (JsonObject) itemV;
			long dsId = item.getJsonNumber("dsId").longValueExact();
			String dsLocation = item.isNull("dsLocation") ? null : item.getString("dsLocation");
			dsInfos.put(
					dsId,
					new DsInfoImpl(dsId, item.getString("dsName"), dsLocation, item.getJsonNumber(
							"invId").longValueExact(), item.getString("invName"), item
							.getString("visitId"), item.getJsonNumber("facilityId")
							.longValueExact(), item.getString("facilityName")));
		}
		prepared.dsInfos = dsInfos;

		for (JsonValue itemV : pd.getJsonArray("emptyDs")) {
			emptyDatasets.add(((JsonNumber) itemV).longValueExact());
		}
		prepared.emptyDatasets = emptyDatasets;

		return prepared;
	}

	public Response getData(String preparedId, String outname, final long offset)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException {

		// Log and validate
		logger.info("New webservice request: getData preparedId = '" + preparedId + "' outname = '"
				+ outname + "' offset = " + offset);

		validateUUID("sessionId", preparedId);

		// Do it
		Prepared prepared;
		try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
			prepared = unpack(stream);
		} catch (NoSuchFileException e) {
			throw new NotFoundException("The preparedId " + preparedId + " is not known");
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		final boolean zip = prepared.zip;
		final boolean compress = prepared.compress;
		final Set<DfInfoImpl> dfInfos = prepared.dfInfos;
		final Map<Long, DsInfo> dsInfos = prepared.dsInfos;
		Set<Long> emptyDatasets = prepared.emptyDatasets;

		/*
		 * Lock the datasets which prevents deletion of datafiles within the dataset and archiving
		 * of the datasets. It is important that they be unlocked again.
		 */
		final String lockId = fsm.lock(dsInfos.keySet());
		if (twoLevel) {
			try { // They can be archived after isPrepared has been called as this does not lock
					// things
				boolean restoreNeeded = false;
				for (DsInfo dsInfo : dsInfos.values()) {
					if (!emptyDatasets.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
						fsm.queue(dsInfo, DeferredOp.RESTORE);
						restoreNeeded = true;
					}
				}
				if (restoreNeeded) {
					fsm.unlock(lockId);
					throw new DataNotOnlineException(
							"Before getting a datafile, its dataset has to be restored, restoration requested automatically");
				}

			} catch (IOException e) {
				fsm.unlock(lockId);
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		checkDatafilesPresent(dfInfos, lockId);

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {

				try {
					if (offset != 0) { // Wrap if needed
						output = new RangeOutputStream(output, offset, null);
					}

					byte[] bytes = new byte[BUFSIZ];
					if (zip) {
						ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(output));
						if (!compress) {
							zos.setLevel(0); // Otherwise use default compression
						}

						for (DfInfoImpl dfInfo : dfInfos) {
							logger.debug("Adding " + dfInfo + " to zip");
							DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
							String entryName = zipMapper.getFullEntryName(dsInfo, dfInfo);
							zos.putNextEntry(new ZipEntry(entryName));
							InputStream stream = mainStorage.get(dfInfo.getDfLocation(),
									dfInfo.getCreateId(), dfInfo.getModId());

							int length;
							while ((length = stream.read(bytes)) >= 0) {
								zos.write(bytes, 0, length);
							}
							zos.closeEntry();
							stream.close();
						}
						zos.close();
					} else {
						DfInfoImpl dfInfo = dfInfos.iterator().next();
						InputStream stream = mainStorage.get(dfInfo.getDfLocation(),
								dfInfo.getCreateId(), dfInfo.getModId());
						int length;
						while ((length = stream.read(bytes)) >= 0) {
							output.write(bytes, 0, length);
						}
						output.close();
					}
				} finally {
					fsm.unlock(lockId);
				}

			}
		};

		/* Construct the name to include in the headers */
		String name;
		if (outname == null) {
			if (zip) {
				name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) + ".zip";
			} else {
				name = dfInfos.iterator().next().getDfName();
			}
		} else {
			if (zip) {
				String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
				if ("zip".equals(ext)) {
					name = outname;
				} else {
					name = outname + ".zip";
				}
			} else {
				name = outname;
			}
		}
		return Response
				.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();

	}

	private void checkDatafilesPresent(Set<? extends DfInfo> dfInfos, String lockId)
			throws NotFoundException, InternalException {
		/* Check that datafiles have not been deleted before locking */
		int n = 0;
		int nmax = 1000;
		StringBuffer sb = new StringBuffer("SELECT COUNT(df) from Datafile df WHERE (df.id in (");
		for (DfInfo dfInfo : dfInfos) {
			if (n != 0) {
				sb.append(',');
			}
			sb.append(dfInfo.getDfId());
			if (n++ == nmax) {
				try {
					if (((Long) reader.search(sb.append("))").toString()).get(0)).intValue() != n) {
						fsm.unlock(lockId);
						throw new NotFoundException(
								"One of the datafiles requested has been deleted");
					}
					n = 0;
					sb = new StringBuffer("SELECT COUNT(df) from Datafile df WHERE (df.id in (");
				} catch (IcatException_Exception e) {
					fsm.unlock(lockId);
					throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
				}
			}
		}
		if (n != 0) {
			try {
				if (((Long) reader.search(sb.append("))").toString()).get(0)).intValue() != n) {
					fsm.unlock(lockId);
					throw new NotFoundException("One of the datafiles requested has been deleted");
				}
			} catch (IcatException_Exception e) {
				fsm.unlock(lockId);
				throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
			}
		}

	}

	public Response getData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, final boolean compress, boolean zip, String outname,
			final long offset) throws BadRequestException, NotImplementedException,
			InternalException, InsufficientPrivilegesException, NotFoundException,
			DataNotOnlineException {

		// Log and validate
		logger.info(String
				.format("New webservice request: getData investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

		/*
		 * Lock the datasets which prevents deletion of datafiles within the dataset and archiving
		 * of the datasets. It is important that they be unlocked again.
		 */

		final String lockId = fsm.lock(dsInfos.keySet());

		if (twoLevel) {
			try {
				boolean restoreNeeded = false;

				Set<Long> emptyDatasets = dataSelection.getEmptyDatasets();
				for (DsInfo dsInfo : dsInfos.values()) {
					if (!emptyDatasets.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
						fsm.queue(dsInfo, DeferredOp.RESTORE);
						restoreNeeded = true;
					}
				}
				if (restoreNeeded) {
					fsm.unlock(lockId);
					throw new DataNotOnlineException(
							"Before getting a datafile, its dataset has to be restored, restoration requested automatically");
				}

			} catch (IOException e) {
				fsm.unlock(lockId);
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		checkDatafilesPresent(dfInfos, lockId);

		final boolean finalZip = zip ? true : dataSelection.mustZip();

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {

				try {
					if (offset != 0) { // Wrap if needed
						output = new RangeOutputStream(output, offset, null);
					}

					byte[] bytes = new byte[BUFSIZ];
					if (finalZip) {
						ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(output));
						if (!compress) {
							zos.setLevel(0); // Otherwise use default compression
						}

						Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
						for (DfInfoImpl dfInfo : dataSelection.getDfInfo()) {
							logger.debug("Adding " + dfInfo + " to zip");
							DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
							String entryName = zipMapper.getFullEntryName(dsInfo, dfInfo);
							zos.putNextEntry(new ZipEntry(entryName));
							InputStream stream = mainStorage.get(dfInfo.getDfLocation(),
									dfInfo.getCreateId(), dfInfo.getModId());

							int length;
							while ((length = stream.read(bytes)) >= 0) {
								zos.write(bytes, 0, length);
							}
							zos.closeEntry();
							stream.close();
						}
						zos.close();
					} else {
						DfInfoImpl dfInfo = dataSelection.getDfInfo().iterator().next();
						InputStream stream = mainStorage.get(dfInfo.getDfLocation(),
								dfInfo.getCreateId(), dfInfo.getModId());
						int length;
						while ((length = stream.read(bytes)) >= 0) {
							output.write(bytes, 0, length);
						}
						output.close();
					}
				} finally {
					fsm.unlock(lockId);
				}
			}
		};

		/* Construct the name to include in the headers */
		String name;
		if (outname == null) {
			if (finalZip) {
				name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) + ".zip";
			} else {
				name = dataSelection.getDfInfo().iterator().next().getDfName();
			}
		} else {
			if (finalZip) {
				String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
				if ("zip".equals(ext)) {
					name = outname;
				} else {
					name = outname + ".zip";
				}
			} else {
				name = outname;
			}
		}
		return Response
				.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();
	}

	public Boolean isPrepared(String preparedId) throws BadRequestException, NotFoundException,
			InternalException {

		logger.info(String.format("New webservice request: isPrepared preparedId=%s", preparedId));

		// Validate
		validateUUID("preparedId", preparedId);

		// Do it
		boolean prepared = true;

		Prepared preparedJson;
		try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
			preparedJson = unpack(stream);
		} catch (NoSuchFileException e) {
			throw new NotFoundException("The preparedId " + preparedId + " is not known");
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (twoLevel) {
			Map<Long, DsInfo> dsInfos = preparedJson.dsInfos;
			Set<Long> emptyDatasets = preparedJson.emptyDatasets;
			try {
				for (DsInfo dsInfo : dsInfos.values()) {
					if (fsm.getRestoring().contains(dsInfo)) {
						prepared = false;
					} else if (!emptyDatasets.contains(dsInfo.getDsId())
							&& !mainStorage.exists(dsInfo)) {
						fsm.queue(dsInfo, DeferredOp.RESTORE);
						prepared = false;
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}
		return prepared;

	}

	public String getStatus(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws BadRequestException, NotFoundException,
			InsufficientPrivilegesException, InternalException {

		// Log and validate
		logger.info(String
				.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS);

		// Do it
		Status status = Status.ONLINE;
		if (twoLevel) {

			try {
				Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
				/*
				 * Restoring shows also data sets which are currently being changed so it may
				 * indicate that something is restoring when it should have been marked as archived.
				 */
				Set<DsInfo> restoring = fsm.getRestoring();
				for (DsInfo dsInfo : dsInfos.values()) {
					if (!mainStorage.exists(dsInfo)) {
						if (status == Status.ONLINE) {
							if (restoring.contains(dsInfo)) {
								status = Status.RESTORING;
							} else {
								status = Status.ARCHIVED;
							}
						} else if (status == Status.RESTORING) {
							if (!restoring.contains(dsInfo)) {
								status = Status.ARCHIVED;
								break;
							}
						}
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}

		}
		logger.debug("Status is " + status.name());
		return status.name();

	}

	@PostConstruct
	private void init() {
		try {
			synchronized (inited) {
				logger.info("creating IdsBean");
				propertyHandler = PropertyHandler.getInstance();
				zipMapper = propertyHandler.getZipMapper();
				mainStorage = propertyHandler.getMainStorage();
				twoLevel = propertyHandler.getArchiveStorage() != null;
				datatypeFactory = DatatypeFactory.newInstance();
				preparedDir = propertyHandler.getCacheDir().resolve("prepared");
				Files.createDirectories(preparedDir);
				linkDir = propertyHandler.getCacheDir().resolve("link");
				Files.createDirectories(linkDir);

				rootUserNames = propertyHandler.getRootUserNames();
				readOnly = propertyHandler.getReadOnly();

				icat = propertyHandler.getIcatService();

				if (twoLevel) {
					datasetDir = propertyHandler.getCacheDir().resolve("dataset");
					markerDir = propertyHandler.getCacheDir().resolve("marker");
					if (!inited) {
						Files.createDirectories(datasetDir);
						Files.createDirectories(markerDir);
						restartUnfinishedWork();
					}
				}

				if (!inited) {
					cleanPreparedDir(preparedDir);
					if (twoLevel) {
						cleanDatasetCache(datasetDir);
					}
				}

				linkEnabled = propertyHandler.getlinkLifetimeMillis() > 0;

				inited = true;

				logger.info("created IdsBean");
			}
		} catch (Exception e) {
			logger.error("Won't start " + (e.getStackTrace())[0]);
			throw new RuntimeException("IdsBean reports " + e.getClass() + " " + e.getMessage());
		}
	}

	static void cleanPreparedDir(Path preparedDir) {
		for (File file : preparedDir.toFile().listFiles()) {
			Path path = file.toPath();
			String pf = path.getFileName().toString();
			if (pf.startsWith("tmp.") || pf.endsWith(".tmp")) {
				try {
					long thisSize = 0;
					if (Files.isDirectory(path)) {
						for (File notZipFile : file.listFiles()) {
							thisSize += Files.size(notZipFile.toPath());
							Files.delete(notZipFile.toPath());
						}
					}
					thisSize += Files.size(path);
					Files.delete(path);
					logger.debug("Deleted " + path + " to reclaim " + thisSize + " bytes");
				} catch (IOException e) {
					logger.debug("Failed to delete " + path + e.getMessage());
				}
			}
		}
	}

	static void cleanDatasetCache(Path datasetDir) {
		for (File dsFile : datasetDir.toFile().listFiles()) {
			Path path = dsFile.toPath();
			try {
				long thisSize = Files.size(path);
				Files.delete(path);
				logger.debug("Deleted " + path + " to reclaim " + thisSize + " bytes");
			} catch (IOException e) {
				logger.debug("Failed to delete " + path + " " + e.getClass() + " " + e.getMessage());
			}
		}
	}

	public String prepareData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, boolean compress, boolean zip) throws NotImplementedException,
			BadRequestException, InternalException, InsufficientPrivilegesException,
			NotFoundException {

		// Log and validate
		logger.info("New webservice request: prepareData " + "investigationIds='"
				+ investigationIds + "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='"
				+ datafileIds + "' " + "compress='" + compress + "' " + "zip='" + zip + "'");

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		String preparedId = UUID.randomUUID().toString();

		if (twoLevel) {

			Collection<DsInfo> dsInfos = dataSelection.getDsInfo().values();
			Set<Long> emptyDs = dataSelection.getEmptyDatasets();

			try {
				for (DsInfo dsInfo : dsInfos) {
					if (fsm.getRestoring().contains(dsInfo)) {
						logger.debug("Restore of " + dsInfo + " is requested or in progress");
					} else if (!emptyDs.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
						logger.debug("Queueing restore of " + dsInfo + " for preparedId: "
								+ preparedId);
						fsm.queue(dsInfo, DeferredOp.RESTORE);
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		if (dataSelection.mustZip()) {
			zip = true;
		}

		logger.debug("Writing to " + preparedDir.resolve(preparedId));
		try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(preparedDir
				.resolve(preparedId)))) {
			pack(stream, zip, compress, dataSelection.getDsInfo(), dataSelection.getDfInfo(),
					dataSelection.getEmptyDatasets());
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		logger.debug("preparedId is " + preparedId);

		return preparedId;
	}

	static void pack(OutputStream stream, boolean zip, boolean compress, Map<Long, DsInfo> dsInfos,
			Set<DfInfoImpl> dfInfos, Set<Long> emptyDatasets) {
		JsonGenerator gen = Json.createGenerator(stream);
		gen.writeStartObject();
		gen.write("zip", zip);
		gen.write("compress", compress);

		gen.writeStartArray("dsInfo");
		for (DsInfo dsInfo : dsInfos.values()) {
			logger.debug("dsInfo " + dsInfo);
			gen.writeStartObject().write("dsId", dsInfo.getDsId())

			.write("dsName", dsInfo.getDsName()).write("facilityId", dsInfo.getFacilityId())
					.write("facilityName", dsInfo.getFacilityName())
					.write("invId", dsInfo.getInvId()).write("invName", dsInfo.getInvName())
					.write("visitId", dsInfo.getVisitId());
			if (dsInfo.getDsLocation() != null) {
				gen.write("dsLocation", dsInfo.getDsLocation());
			} else {
				gen.writeNull("dsLocation");
			}
			gen.writeEnd();
		}
		gen.writeEnd();

		gen.writeStartArray("dfInfo");
		for (DfInfoImpl dfInfo : dfInfos) {
			DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
			gen.writeStartObject().write("dsId", dsInfo.getDsId()).write("dfId", dfInfo.getDfId())
					.write("dfName", dfInfo.getDfName()).write("createId", dfInfo.getCreateId())
					.write("modId", dfInfo.getModId());
			if (dfInfo.getDfLocation() != null) {
				gen.write("dfLocation", dfInfo.getDfLocation());
			} else {
				gen.writeNull("dfLocation");
			}
			gen.writeEnd();

		}
		gen.writeEnd();

		gen.writeStartArray("emptyDs");
		for (Long emptyDs : emptyDatasets) {
			gen.write(emptyDs);
		}
		gen.writeEnd();

		gen.writeEnd().close();

	}

	public Response put(InputStream body, String sessionId, String name, long datafileFormatId,
			long datasetId, String description, String doi, Long datafileCreateTime,
			Long datafileModTime, boolean wrap, boolean padding) throws NotFoundException,
			DataNotOnlineException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotImplementedException {

		try {
			// Log and validate
			logger.info("New webservice request: put " + "name='" + name + "' "
					+ "datafileFormatId='" + datafileFormatId + "' " + "datasetId='" + datasetId
					+ "' " + "description='" + description + "' " + "doi='" + doi + "' "
					+ "datafileCreateTime='" + datafileCreateTime + "' " + "datafileModTime='"
					+ datafileModTime + "'");

			if (readOnly) {
				throw new NotImplementedException(
						"This operation has been configured to be unavailable");
			}

			IdsBean.validateUUID("sessionId", sessionId);
			if (name == null) {
				throw new BadRequestException("The name parameter must be set");
			}
			if (datafileFormatId == 0) {
				throw new BadRequestException("The datafileFormatId parameter must be set");
			}
			if (datasetId == 0) {
				throw new BadRequestException("The datasetId parameter must be set");
			}

			// Do it
			Dataset ds;
			try {
				ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Investigation, Facility",
						datasetId);
			} catch (IcatException_Exception e) {
				IcatExceptionType type = e.getFaultInfo().getType();
				if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
						|| type == IcatExceptionType.SESSION) {
					throw new InsufficientPrivilegesException(e.getMessage());
				}
				if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
					throw new NotFoundException(e.getMessage());
				}
				throw new InternalException(type + " " + e.getMessage());
			}

			DsInfo dsInfo = new DsInfoImpl(ds);
			try {

				if (twoLevel) {
					if (!mainStorage.exists(dsInfo)) {
						try {
							List<Object> counts = icat.search(sessionId,
									"COUNT(Datafile) <-> Dataset [id=" + dsInfo.getDsId() + "]");
							if ((Long) counts.get(0) != 0) {
								fsm.queue(dsInfo, DeferredOp.RESTORE);
								throw new DataNotOnlineException(
										"Before putting a datafile, its dataset has to be restored, restoration requested automatically");
							}
						} catch (IcatException_Exception e) {
							IcatExceptionType type = e.getFaultInfo().getType();
							if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
									|| type == IcatExceptionType.SESSION) {
								throw new InsufficientPrivilegesException(e.getMessage());
							}
							if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
								throw new NotFoundException(e.getMessage());
							}
							throw new InternalException(type + " " + e.getMessage());
						}

					}
				}

				CRC32 crc = new CRC32();
				CheckedWithSizeInputStream is = new CheckedWithSizeInputStream(body, crc);
				String location = mainStorage.put(dsInfo, name, is);
				is.close();
				long checksum = crc.getValue();
				long size = is.getSize();
				Long dfId;
				try {
					dfId = registerDatafile(sessionId, name, datafileFormatId, location, checksum,
							size, ds, description, doi, datafileCreateTime, datafileModTime);
				} catch (InsufficientPrivilegesException | NotFoundException | InternalException
						| BadRequestException e) {
					logger.debug("Problem with registration " + e.getClass() + " " + e.getMessage()
							+ " datafile will now be deleted");
					mainStorage.delete(location);
					throw e;
				}

				if (twoLevel) {
					fsm.queue(dsInfo, DeferredOp.WRITE);
				}
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				Json.createGenerator(baos).writeStartObject().write("id", dfId)
						.write("checksum", checksum)
						.write("location", location.replace("\\", "\\\\").replace("'", "\\'"))
						.write("size", size).writeEnd().close();
				if (wrap) {
					return Response.status(HttpURLConnection.HTTP_CREATED)
							.entity(prefix + baos.toString() + suffix).build();
				} else {
					return Response.status(HttpURLConnection.HTTP_CREATED).entity(baos.toString())
							.build();
				}

			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		} catch (IdsException e) {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JsonGenerator gen = Json.createGenerator(baos);
			gen.writeStartObject().write("code", e.getClass().getSimpleName())
					.write("message", e.getShortMessage());
			gen.writeEnd().close();
			if (wrap) {
				String pre = padding ? paddedPrefix : prefix;
				return Response.status(e.getHttpStatusCode())
						.entity(pre + baos.toString().replace("'", "\\'") + suffix).build();
			} else {
				return Response.status(e.getHttpStatusCode()).entity(baos.toString()).build();
			}
		}

	}

	private Long registerDatafile(String sessionId, String name, long datafileFormatId,
			String location, long checksum, long size, Dataset dataset, String description,
			String doi, Long datafileCreateTime, Long datafileModTime)
			throws InsufficientPrivilegesException, NotFoundException, InternalException,
			BadRequestException {
		final Datafile df = new Datafile();
		DatafileFormat format;
		try {
			format = (DatafileFormat) icat.get(sessionId, "DatafileFormat", datafileFormatId);
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		}

		df.setDatafileFormat(format);
		df.setLocation(location);
		df.setFileSize(size);
		df.setChecksum(Long.toHexString(checksum));
		df.setName(name);
		df.setDataset(dataset);
		df.setDescription(description);
		df.setDoi(doi);
		if (datafileCreateTime != null) {
			GregorianCalendar gregorianCalendar = new GregorianCalendar();
			gregorianCalendar.setTimeInMillis(datafileCreateTime);
			df.setDatafileCreateTime(datatypeFactory.newXMLGregorianCalendar(gregorianCalendar));
		}
		if (datafileModTime != null) {
			GregorianCalendar gregorianCalendar = new GregorianCalendar();
			gregorianCalendar.setTimeInMillis(datafileModTime);
			df.setDatafileModTime(datatypeFactory.newXMLGregorianCalendar(gregorianCalendar));
		}
		try {
			df.setId(icat.create(sessionId, df));
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.VALIDATION) {
				throw new BadRequestException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		}
		logger.debug("Registered datafile for dataset {} for {}", dataset.getId(), name + " at "
				+ location);
		return df.getId();
	}

	private void restartUnfinishedWork() throws InternalException {
		List<String> creds = propertyHandler.getReader();

		Credentials credentials = new Credentials();
		List<Entry> entries = credentials.getEntry();
		for (int i = 1; i < creds.size(); i += 2) {
			Entry entry = new Entry();
			entry.setKey(creds.get(i));
			entry.setValue(creds.get(i + 1));
			entries.add(entry);
		}

		try {
			String sessionId = icat.login(creds.get(0), credentials);
			for (File file : markerDir.toFile().listFiles()) {
				long dsid = Long.parseLong(file.toPath().getFileName().toString());
				Dataset ds = null;
				try {
					ds = (Dataset) icat.get(sessionId,
							"Dataset ds INCLUDE ds.datafiles, ds.investigation.facility", dsid);
					DsInfo dsInfo = new DsInfoImpl(ds);
					fsm.queue(dsInfo, DeferredOp.WRITE);
					logger.info("Queued dataset with id " + dsid + " " + dsInfo
							+ " to be written as it was not written out previously by IDS");
				} catch (IcatException_Exception e) {
					if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
						logger.warn("Dataset with id " + dsid
								+ " was not written out by IDS and now no longer known to ICAT");
						Files.delete(file.toPath());
					} else {
						throw e;
					}
				}
			}
		} catch (Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public void restore(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws BadRequestException, NotImplementedException,
			InsufficientPrivilegesException, InternalException, NotFoundException {

		// Log and validate
		logger.info("New webservice request: restore " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS);

		// Do it

		if (twoLevel) {
			Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos.values()) {
				fsm.queue(dsInfo, DeferredOp.RESTORE);
			}
		}
	}

	public String getServiceStatus(String sessionId) throws InternalException,
			InsufficientPrivilegesException {

		// Log and validate
		logger.info("New webservice request: getServiceStatus");

		try {
			String uname = icat.getUserName(sessionId);
			if (!rootUserNames.contains(uname)) {
				throw new InsufficientPrivilegesException(uname
						+ " is not included in the ids rootUserNames set.");
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getClass() + " " + e.getMessage());
			}
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
		return fsm.getServiceStatus();
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public boolean isTwoLevel() {
		return twoLevel;
	}

	public long getSize(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws BadRequestException, NotFoundException,
			InsufficientPrivilegesException, InternalException {

		// Log and validate
		logger.info(String
				.format("New webservice request: getSize investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		long size = 0;
		StringBuilder sb = new StringBuilder();
		int n = 0;
		for (DfInfoImpl df : dataSelection.getDfInfo()) {
			if (sb.length() != 0) {
				sb.append(',');
			}
			sb.append(df.getDfId());
			if (n++ == 100) {
				size += getSizeFor(sessionId, sb);
				sb = new StringBuilder();
				n = 0;
			}
		}
		if (n > 0) {
			size += getSizeFor(sessionId, sb);
		}
		return size;
	}

	private long getSizeFor(String sessionId, StringBuilder sb) throws InternalException {
		String query = "SELECT SUM(df.fileSize) from Datafile df WHERE df.id IN (" + sb.toString()
				+ ")";
		try {
			return (Long) icat.search(sessionId, query).get(0);
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} catch (IndexOutOfBoundsException e) {
			return 0L;
		}
	}

	public String getLink(String sessionId, long datafileId, String username)
			throws BadRequestException, InsufficientPrivilegesException, InternalException,
			NotFoundException, DataNotOnlineException, NotImplementedException {
		// Log and validate
		logger.info("New webservice request: getLink datafileId=" + datafileId + " username='"
				+ username + "'");

		if (!linkEnabled) {
			throw new NotImplementedException(
					"Sorry getLink is not available on this IDS installation");
		}

		validateUUID("sessionId", sessionId);

		Datafile datafile = null;
		try {
			datafile = (Datafile) icat.get(sessionId,
					"Datafile INCLUDE Dataset, Investigation, Facility", datafileId);
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.BAD_PARAMETER) {
				throw new BadRequestException(e.getMessage());
			} else if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES) {
				throw new InsufficientPrivilegesException(e.getMessage());
			} else if (type == IcatExceptionType.INTERNAL) {
				throw new InternalException(e.getMessage());
			} else if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			} else if (type == IcatExceptionType.OBJECT_ALREADY_EXISTS) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			} else if (type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			} else if (type == IcatExceptionType.VALIDATION) {
				throw new BadRequestException(e.getMessage());
			}
		}

		if (twoLevel) {
			try {
				DsInfo dsInfo = new DsInfoImpl(datafile.getDataset());
				if (!mainStorage.exists(dsInfo)) {
					fsm.queue(dsInfo, DeferredOp.RESTORE);
					throw new DataNotOnlineException(
							"Before linking a datafile, its dataset has to be restored, restoration requested automatically");
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		try {
			Path target = mainStorage.getPath(datafile.getLocation(), datafile.getCreateId(),
					datafile.getModId());
			ShellCommand sc = new ShellCommand("setfacl", "-m", "user:" + username + ":r",
					target.toString());
			if (sc.getExitValue() != 0) {
				throw new BadRequestException(sc.getMessage() + ". Check that user '" + username
						+ "' exists");
			}
			Path link = linkDir.resolve(UUID.randomUUID().toString());
			Files.createLink(link, target);
			return link.toString();
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

	}

}
