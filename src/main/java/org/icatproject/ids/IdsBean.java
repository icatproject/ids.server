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
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
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
import org.icatproject.ids.DataSelection.Returns;
import org.icatproject.ids.FiniteStateMachine.SetLockType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.IdsException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.ArchiveStorageInterface;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.utils.IcatSecurity;
import org.icatproject.utils.ShellCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
public class IdsBean {

	public class RunPrepDsCheck implements Callable<Void> {

		private Collection<DsInfo> toCheck;
		private Set<Long> emptyDatasets;

		public RunPrepDsCheck(Collection<DsInfo> toCheck, Set<Long> emptyDatasets) {
			this.toCheck = toCheck;
			this.emptyDatasets = emptyDatasets;
		}

		@Override
		public Void call() throws Exception {
			for (DsInfo dsInfo : toCheck) {
				fsm.checkFailure(dsInfo.getDsId());
				restoreIfOffline(dsInfo, emptyDatasets);
			}
			return null;
		}

	}

	public class RunPrepDfCheck implements Callable<Void> {

		private SortedSet<DfInfoImpl> toCheck;

		public RunPrepDfCheck(SortedSet<DfInfoImpl> toCheck) {
			this.toCheck = toCheck;
		}

		@Override
		public Void call() throws Exception {
			for (DfInfoImpl dfInfo : toCheck) {
				fsm.checkFailure(dfInfo.getDfId());
				restoreIfOffline(dfInfo);
			}
			return null;
		}

	}

	enum CallType {
		INFO, PREPARE, READ, WRITE, MIGRATE, LINK
	};

	public class RestoreDfTask implements Callable<Void> {

		private Set<DfInfoImpl> dfInfos;

		public RestoreDfTask(Set<DfInfoImpl> dfInfos) {
			this.dfInfos = dfInfos;
		}

		@Override
		public Void call() throws Exception {
			for (DfInfoImpl dfInfo : dfInfos) {
				try {
					restoreIfOffline(dfInfo);
				} catch (IOException e) {
					logger.error("I/O error " + e.getMessage() + " for " + dfInfo);
				}
			}
			return null;
		}

	}

	public class RestoreDsTask implements Callable<Void> {
		private Collection<DsInfo> dsInfos;
		private Set<Long> emptyDs;

		public RestoreDsTask(Collection<DsInfo> dsInfos, Set<Long> emptyDs) {
			this.dsInfos = dsInfos;
			this.emptyDs = emptyDs;
		}

		@Override
		public Void call() throws Exception {
			for (DsInfo dsInfo : dsInfos) {
				try {
					restoreIfOffline(dsInfo, emptyDs);
				} catch (IOException e) {
					logger.error("I/O error " + e.getMessage() + " for " + dsInfo);
				}
			}
			return null;
		}
	}

	private class SO implements StreamingOutput {

		private long offset;
		private boolean zip;
		private Map<Long, DsInfo> dsInfos;
		private String lockId;
		private boolean compress;
		private Set<DfInfoImpl> dfInfos;
		private String ip;
		private long start;
		private Long transferId;

		SO(Map<Long, DsInfo> dsInfos, Set<DfInfoImpl> dfInfos, long offset, boolean zip, boolean compress,
				String lockId, Long transferId, String ip, long start) {
			this.offset = offset;
			this.zip = zip;
			this.dsInfos = dsInfos;
			this.dfInfos = dfInfos;
			this.lockId = lockId;
			this.compress = compress;
			this.transferId = transferId;
			this.ip = ip;
			this.start = start;
		}

		@Override
		public void write(OutputStream output) throws IOException {
			Object transfer = "??";
			try {
				if (offset != 0) { // Wrap the stream if needed
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
						transfer = dfInfo;
						DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
						String entryName = zipMapper.getFullEntryName(dsInfo, dfInfo);
						InputStream stream = null;
						try {
							zos.putNextEntry(new ZipEntry(entryName));
							stream = mainStorage.get(dfInfo.getDfLocation(), dfInfo.getCreateId(), dfInfo.getModId());
							int length;
							while ((length = stream.read(bytes)) >= 0) {
								zos.write(bytes, 0, length);
							}
						} catch (ZipException e) {
							logger.debug("Skipped duplicate");
						}
						zos.closeEntry();
						if (stream != null) {
							stream.close();
						}
					}
					zos.close();
				} else {
					DfInfoImpl dfInfo = dfInfos.iterator().next();
					transfer = dfInfo;
					InputStream stream = mainStorage.get(dfInfo.getDfLocation(), dfInfo.getCreateId(),
							dfInfo.getModId());
					int length;
					while ((length = stream.read(bytes)) >= 0) {
						output.write(bytes, 0, length);
					}
					output.close();
					stream.close();
				}

				if (transferId != null) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
						gen.write("transferId", transferId);
						gen.writeEnd();
					}
					transmitter.processMessage("getData", ip, baos.toString(), start);
				}

			} catch (IOException e) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("transferId", transferId);
					gen.write("exceptionClass", e.getClass().toString());
					gen.write("exceptionMessage", e.getMessage());
					gen.writeEnd();
				}
				transmitter.processMessage("getData", ip, baos.toString(), start);
				logger.error("Failed to stream " + transfer + " due to " + e.getMessage());
				throw e;
			} finally {
				fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
			}
		}

	}

	private static final int BUFSIZ = 2048;

	private static Boolean inited = false;

	private static String key;

	private final static Logger logger = LoggerFactory.getLogger(IdsBean.class);
	private static String paddedPrefix;
	private static final String prefix = "<html><script type=\"text/javascript\">window.name='";
	private static final String suffix = "';</script></html>";

	/** matches standard UUID format of 8-4-4-4-12 hexadecimal digits */
	public static final Pattern uuidRegExp = Pattern
			.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

	static {
		paddedPrefix = "<html><script type=\"text/javascript\">/*";
		for (int n = 1; n < 25; n++) {
			paddedPrefix += " *        \n";
		}
		paddedPrefix += "*/window.name='";
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

	public static String getLocation(long dfid, String location)
			throws InsufficientPrivilegesException, InternalException {
		if (location == null) {
			throw new InsufficientPrivilegesException("location null");
		}
		if (key == null) {
			return location;
		} else {
			return getLocationFromDigest(dfid, location, key);
		}
	}

	static String getLocationFromDigest(long id, String locationWithHash, String key)
			throws InternalException, InsufficientPrivilegesException {
		int i = locationWithHash.lastIndexOf(' ');
		try {
			String location = locationWithHash.substring(0, i);
			String hash = locationWithHash.substring(i + 1);
			if (!hash.equals(IcatSecurity.digest(id, location, key))) {
				throw new InsufficientPrivilegesException(
						"Location \"" + locationWithHash + "\" does not contain a valid hash.");
			}
			return location;
		} catch (IndexOutOfBoundsException e) {
			throw new InsufficientPrivilegesException("Location \"" + locationWithHash + "\" does not contain hash.");
		} catch (NoSuchAlgorithmException e) {
			throw new InternalException(e.getMessage());
		}
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
					.write("facilityName", dsInfo.getFacilityName()).write("invId", dsInfo.getInvId())
					.write("invName", dsInfo.getInvName()).write("visitId", dsInfo.getVisitId());
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

	static Prepared unpack(InputStream stream) throws InternalException {
		Prepared prepared = new Prepared();
		JsonObject pd;
		try (JsonReader jsonReader = Json.createReader(stream)) {
			pd = jsonReader.readObject();
		}
		prepared.zip = pd.getBoolean("zip");
		prepared.compress = pd.getBoolean("compress");
		SortedMap<Long, DsInfo> dsInfos = new TreeMap<>();
		SortedSet<DfInfoImpl> dfInfos = new TreeSet<>();
		Set<Long> emptyDatasets = new HashSet<>();

		for (JsonValue itemV : pd.getJsonArray("dfInfo")) {
			JsonObject item = (JsonObject) itemV;
			String dfLocation = item.isNull("dfLocation") ? null : item.getString("dfLocation");
			dfInfos.add(new DfInfoImpl(item.getJsonNumber("dfId").longValueExact(), item.getString("dfName"),
					dfLocation, item.getString("createId"), item.getString("modId"),
					item.getJsonNumber("dsId").longValueExact()));

		}
		prepared.dfInfos = dfInfos;

		for (JsonValue itemV : pd.getJsonArray("dsInfo")) {
			JsonObject item = (JsonObject) itemV;
			long dsId = item.getJsonNumber("dsId").longValueExact();
			String dsLocation = item.isNull("dsLocation") ? null : item.getString("dsLocation");
			dsInfos.put(dsId, new DsInfoImpl(dsId, item.getString("dsName"), dsLocation,
					item.getJsonNumber("invId").longValueExact(), item.getString("invName"), item.getString("visitId"),
					item.getJsonNumber("facilityId").longValueExact(), item.getString("facilityName")));
		}
		prepared.dsInfos = dsInfos;

		for (JsonValue itemV : pd.getJsonArray("emptyDs")) {
			emptyDatasets.add(((JsonNumber) itemV).longValueExact());
		}
		prepared.emptyDatasets = emptyDatasets;

		return prepared;
	}

	public static void validateUUID(String thing, String id) throws BadRequestException {
		if (id == null || !uuidRegExp.matcher(id).matches())
			throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
	}

	private static AtomicLong atomicLong = new AtomicLong();

	@EJB
	Transmitter transmitter;

	private ExecutorService threadPool;

	private ArchiveStorageInterface archiveStorage;

	private Path datasetDir;

	private DatatypeFactory datatypeFactory;

	@EJB
	private FiniteStateMachine fsm;

	private ICAT icat;

	private Path linkDir;

	private boolean linkEnabled;

	private MainStorageInterface mainStorage;

	private Path markerDir;

	private Path preparedDir;

	private PropertyHandler propertyHandler;

	@EJB
	IcatReader reader;

	private boolean readOnly;

	private Set<String> rootUserNames;

	private StorageUnit storageUnit;

	private ZipMapperInterface zipMapper;

	private int maxIdsInQuery;

	private boolean twoLevel;

	private Set<CallType> logSet;

	class PreparedStatus {
		public ReentrantLock lock = new ReentrantLock();
		public DfInfoImpl fromDfElement;
		public Future<?> future;
		public Long fromDsElement;
	};

	private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

	private void addIds(JsonGenerator gen, String investigationIds, String datasetIds, String datafileIds)
			throws BadRequestException {
		if (investigationIds != null) {
			gen.writeStartArray("investigationIds");
			for (long invid : DataSelection.getValidIds("investigationIds", investigationIds)) {
				gen.write(invid);
			}
			gen.writeEnd();
		}
		if (datasetIds != null) {
			gen.writeStartArray("datasetIds");
			for (long invid : DataSelection.getValidIds("datasetIds", datasetIds)) {
				gen.write(invid);
			}
			gen.writeEnd();
		}
		if (datafileIds != null) {
			gen.writeStartArray("datafileIds");
			for (long invid : DataSelection.getValidIds("datafileIds", datafileIds)) {
				gen.write(invid);
			}
			gen.writeEnd();
		}
	}

	public void archive(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: archive " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
				+ datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		validateUUID("sessionId", sessionId);

		// Do it
		if (storageUnit == StorageUnit.DATASET) {
			DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds, datafileIds,
					Returns.DATASETS);
			Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos.values()) {
				fsm.queue(dsInfo, DeferredOp.ARCHIVE);
			}
		} else if (storageUnit == StorageUnit.DATAFILE) {
			DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds, datafileIds,
					Returns.DATAFILES);
			Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();
			for (DfInfoImpl dfInfo : dfInfos) {
				fsm.queue(dfInfo, DeferredOp.ARCHIVE);
			}
		}

		if (logSet.contains(CallType.MIGRATE)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("archive", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}
	}

	private void checkDatafilesPresent(Set<? extends DfInfo> dfInfos, String lockId)
			throws NotFoundException, InternalException {
		/* Check that datafiles have not been deleted before locking */
		int n = 0;
		StringBuffer sb = new StringBuffer("SELECT COUNT(df) from Datafile df WHERE (df.id in (");
		for (DfInfo dfInfo : dfInfos) {
			if (n != 0) {
				sb.append(',');
			}
			sb.append(dfInfo.getDfId());
			if (++n == maxIdsInQuery) {
				try {
					if (((Long) reader.search(sb.append("))").toString()).get(0)).intValue() != n) {
						fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
						throw new NotFoundException("One of the data files requested has been deleted");
					}
					n = 0;
					sb = new StringBuffer("SELECT COUNT(df) from Datafile df WHERE (df.id in (");
				} catch (IcatException_Exception e) {
					fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
					throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
				}
			}
		}
		if (n != 0) {
			try {
				if (((Long) reader.search(sb.append("))").toString()).get(0)).intValue() != n) {
					fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
					throw new NotFoundException("One of the datafiles requested has been deleted");
				}
			} catch (IcatException_Exception e) {
				fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
				throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
			}
		}

	}

	private void checkOnlineAndFreeLockOnFailure(Collection<DsInfo> dsInfos, Set<Long> emptyDatasets,
			Set<DfInfoImpl> dfInfos, String lockId, SetLockType lockType)
			throws InternalException, DataNotOnlineException {
		try {
			if (storageUnit == StorageUnit.DATASET) {
				boolean maybeOffline = false;
				for (DsInfo dsInfo : dsInfos) {
					if (restoreIfOffline(dsInfo, emptyDatasets)) {
						maybeOffline = true;
					}
				}
				if (maybeOffline) {
					fsm.unlock(lockId, lockType);
					throw new DataNotOnlineException(
							"Before putting, getting or deleting a datafile, its dataset has to be restored, restoration requested automatically");
				}
			} else if (storageUnit == StorageUnit.DATAFILE) {
				boolean maybeOffline = false;
				for (DfInfoImpl dfInfo : dfInfos) {
					if (restoreIfOffline(dfInfo)) {
						maybeOffline = true;
					}

				}
				if (maybeOffline) {
					fsm.unlock(lockId, lockType);
					throw new DataNotOnlineException(
							"Before getting a datafile, it must be restored, restoration requested automatically");
				}
			}
		} catch (IOException e) {
			fsm.unlock(lockId, lockType);
			logger.error("I/O error " + e.getMessage() + " checking online");
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

	}

	public void delete(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
			NotFoundException, DataNotOnlineException {

		long start = System.currentTimeMillis();

		logger.info("New webservice request: delete " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
				+ datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		if (readOnly) {
			throw new NotImplementedException("This operation has been configured to be unavailable");
		}

		IdsBean.validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds, datafileIds,
				Returns.DATASETS_AND_DATAFILES);

		// Do it
		Collection<DsInfo> dsInfos = dataSelection.getDsInfo().values();
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

		String lockId = null;
		if (storageUnit == StorageUnit.DATASET) {
			/*
			 * Lock the datasets to prevent archiving of the datasets. It is
			 * important that they be unlocked again.
			 */
			Set<Long> dsIds = new HashSet<>();
			for (DsInfo dsInfo : dsInfos) {
				dsIds.add(dsInfo.getDsId());
			}
			lockId = fsm.lock(dsIds, FiniteStateMachine.SetLockType.ARCHIVE);
			checkOnlineAndFreeLockOnFailure(dsInfos, dataSelection.getEmptyDatasets(), dfInfos, lockId,
					FiniteStateMachine.SetLockType.ARCHIVE);
		}

		try {
			for (DsInfo dsInfo : dsInfos) {
				logger.debug("DS " + dsInfo.getDsId() + " " + dsInfo);
				if (fsm.isLocked(dsInfo.getDsId(), FiniteStateMachine.QueryLockType.DELETE)) {
					throw new BadRequestException(
							"Dataset " + dsInfo + " (or a part of it) is currently being streamed to a user");
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

				if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
					throw new InsufficientPrivilegesException(e.getMessage());
				}
				if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
					throw new NotFoundException(e.getMessage());
				}
				throw new InternalException(type + " " + e.getMessage());
			}

			/*
			 * Delete the local copy directly rather than queueing it as it has
			 * been removed from ICAT so will not be accessible to any
			 * subsequent IDS calls.
			 */
			for (DfInfoImpl dfInfo : dataSelection.getDfInfo()) {
				String location = dfInfo.getDfLocation();
				try {
					if ((long) reader
							.search("SELECT COUNT(df) FROM Datafile df WHERE df.location LIKE '" + location + "%'")
							.get(0) == 0) {
						if (mainStorage.exists(location)) {
							logger.debug("Delete physical file " + location + " from main storage");
							mainStorage.delete(location, dfInfo.getCreateId(), dfInfo.getModId());
						}
						if (storageUnit == StorageUnit.DATAFILE) {
							fsm.queue(dfInfo, DeferredOp.DELETE);
						}
					}
				} catch (IcatException_Exception e) {
					throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
				} catch (IOException e) {
					logger.error("I/O error " + e.getMessage() + " deleting " + dfInfo);
					throw new InternalException(e.getClass() + " " + e.getMessage());
				}
			}

			if (storageUnit == StorageUnit.DATASET) {
				for (DsInfo dsInfo : dsInfos) {
					fsm.queue(dsInfo, DeferredOp.WRITE);
				}
			}

		} finally {
			if (lockId != null) {
				fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE);
			}
		}

		if (logSet.contains(CallType.WRITE)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("delete", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}
	}

	public Response getData(String preparedId, String outname, final long offset, String ip) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException {

		long time = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: getData preparedId = '" + preparedId + "' outname = '" + outname
				+ "' offset = " + offset);

		validateUUID("preparedId", preparedId);

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
		 * Lock the datasets which prevents deletion of datafiles within the
		 * dataset and archiving of the datasets. It is important that they be
		 * unlocked again.
		 */
		final String lockId = fsm.lock(dsInfos.keySet(), FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);

		if (twoLevel) {
			checkOnlineAndFreeLockOnFailure(dsInfos.values(), emptyDatasets, dfInfos, lockId,
					FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
		}

		checkDatafilesPresent(dfInfos, lockId);

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

		Long transferId = null;
		if (logSet.contains(CallType.READ)) {
			transferId = atomicLong.getAndIncrement();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.write("transferId", transferId);
				gen.write("preparedId", preparedId);
				gen.writeEnd();
			}
			transmitter.processMessage("getDataStart", ip, baos.toString(), time);
		}

		return Response.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(new SO(dsInfos, dfInfos, offset, zip, compress, lockId, transferId, ip, time))
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"").header("Accept-Ranges", "bytes")
				.build();
	}

	public Response getData(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			final boolean compress, boolean zip, String outname, final long offset, String ip)
			throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
			DataNotOnlineException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info(String.format("New webservice request: getData investigationIds=%s, datasetIds=%s, datafileIds=%s",
				investigationIds, datasetIds, datafileIds));

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
				datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

		/*
		 * Lock the datasets which prevents deletion of datafiles within the
		 * dataset and archiving of the datasets. It is important that they be
		 * unlocked again.
		 */

		final String lockId = fsm.lock(dsInfos.keySet(), FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);

		if (twoLevel) {
			checkOnlineAndFreeLockOnFailure(dsInfos.values(), dataSelection.getEmptyDatasets(), dfInfos, lockId,
					FiniteStateMachine.SetLockType.ARCHIVE_AND_DELETE);
		}

		checkDatafilesPresent(dfInfos, lockId);

		final boolean finalZip = zip ? true : dataSelection.mustZip();

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

		Long transferId = null;
		if (logSet.contains(CallType.READ)) {
			try {
				transferId = atomicLong.getAndIncrement();
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("transferId", transferId);
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				transmitter.processMessage("getDataStart", ip, baos.toString(), start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}

		return Response.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(new SO(dataSelection.getDsInfo(), dataSelection.getDfInfo(), offset, finalZip, compress, lockId,
						transferId, ip, start))
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"").header("Accept-Ranges", "bytes")
				.build();
	}

	public String getDatafileIds(String preparedId, String ip)
			throws BadRequestException, InternalException, NotFoundException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: getDatafileIds preparedId = '" + preparedId);

		validateUUID("preparedId", preparedId);

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

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
			gen.write("zip", zip);
			gen.write("compress", compress);
			gen.writeStartArray("ids");
			for (DfInfoImpl dfInfo : dfInfos) {
				gen.write(dfInfo.getDfId());
			}
			gen.writeEnd().writeEnd().close();
		}
		String resp = baos.toString();

		if (logSet.contains(CallType.INFO)) {
			baos = new ByteArrayOutputStream();
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.write("preparedId", preparedId);
				gen.writeEnd();
			}
			transmitter.processMessage("getDatafileIds", ip, baos.toString(), start);
		}

		return resp;
	}

	public String getDatafileIds(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info(String.format(
				"New webservice request: getDatafileIds investigationIds=%s, datasetIds=%s, datafileIds=%s",
				investigationIds, datasetIds, datafileIds));

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
				datafileIds, Returns.DATAFILES);

		// Do it
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
			gen.writeStartArray("ids");
			for (DfInfoImpl dfInfo : dfInfos) {
				gen.write(dfInfo.getDfId());
			}
			gen.writeEnd().writeEnd().close();
		}
		String resp = baos.toString();

		if (logSet.contains(CallType.INFO)) {
			baos = new ByteArrayOutputStream();
			try {
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				transmitter.processMessage("getDatafileIds", ip, baos.toString(), start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}

		return resp;

	}

	public String getIcatUrl(String ip) {
		if (logSet.contains(CallType.INFO)) {
			transmitter.processMessage("getIcatUrl", ip, "{}", System.currentTimeMillis());
		}
		return propertyHandler.getIcatUrl();
	}

	public String getLink(String sessionId, long datafileId, String username, String ip)
			throws BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException,
			DataNotOnlineException, NotImplementedException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: getLink datafileId=" + datafileId + " username='" + username + "'");

		if (!linkEnabled) {
			throw new NotImplementedException("Sorry getLink is not available on this IDS installation");
		}

		validateUUID("sessionId", sessionId);

		Datafile datafile = null;
		try {
			datafile = (Datafile) icat.get(sessionId, "Datafile INCLUDE Dataset, Investigation, Facility", datafileId);
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

		String location = getLocation(datafile.getId(), datafile.getLocation());

		try {
			if (storageUnit == StorageUnit.DATASET) {
				DsInfo dsInfo = new DsInfoImpl(datafile.getDataset());
				Set<Long> mt = Collections.emptySet();
				if (restoreIfOffline(dsInfo, mt)) {
					throw new DataNotOnlineException(
							"Before linking a datafile, its dataset has to be restored, restoration requested automatically");
				}
			} else if (storageUnit == StorageUnit.DATAFILE) {
				DfInfoImpl dfInfo = new DfInfoImpl(datafileId, datafile.getName(), location, datafile.getCreateId(),
						datafile.getModId(), datafile.getDataset().getId());
				if (restoreIfOffline(dfInfo)) {
					throw new DataNotOnlineException(
							"Before linking a datafile, it has to be restored, restoration requested automatically");
				}
			}
		} catch (IOException e) {
			logger.error("I/O error " + e.getMessage() + " linking " + location + " from MainStorage");
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		try {
			Path target = mainStorage.getPath(location, datafile.getCreateId(), datafile.getModId());
			ShellCommand sc = new ShellCommand("setfacl", "-m", "user:" + username + ":r", target.toString());
			if (sc.getExitValue() != 0) {
				throw new BadRequestException(sc.getMessage() + ". Check that user '" + username + "' exists");
			}
			Path link = linkDir.resolve(UUID.randomUUID().toString());
			Files.createLink(link, target);

			if (logSet.contains(CallType.LINK)) {
				try {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
						gen.write("userName", icat.getUserName(sessionId));
						gen.write("datafileId", datafileId);
						gen.writeEnd();
					}
					String body = baos.toString();
					transmitter.processMessage("getLink", ip, body, start);
				} catch (IcatException_Exception e) {
					logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
				}
			}

			return link.toString();
		} catch (IOException e) {
			logger.error("I/O error " + e.getMessage() + " linking " + location + " from MainStorage");
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

	}

	public String getServiceStatus(String sessionId, String ip)
			throws InternalException, InsufficientPrivilegesException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: getServiceStatus");

		try {
			String uname = icat.getUserName(sessionId);
			if (!rootUserNames.contains(uname)) {
				throw new InsufficientPrivilegesException(uname + " is not included in the ids rootUserNames set.");
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getClass() + " " + e.getMessage());
			}
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (logSet.contains(CallType.INFO)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("getServiceStatus", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}

		return fsm.getServiceStatus();
	}

	public long getSize(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info(String.format("New webservice request: getSize investigationIds=%s, datasetIds=%s, datafileIds=%s",
				investigationIds, datasetIds, datafileIds));

		validateUUID("sessionId", sessionId);

		List<Long> dfids = DataSelection.getValidIds("datafileIds", datafileIds);
		List<Long> dsids = DataSelection.getValidIds("datasetIds", datasetIds);
		List<Long> invids = DataSelection.getValidIds("investigationIds", investigationIds);

		long size = 0;
		if (dfids.size() + dsids.size() + invids.size() == 1) {
			size = getSizeFor(sessionId, invids, "df.dataset.investigation.id")
					+ getSizeFor(sessionId, dsids, "df.dataset.id") + getSizeFor(sessionId, dfids, "df.id");
			logger.debug("Fast computation for simple case");
			if (size == 0) {
				try {
					if (dfids.size() != 0) {
						icat.get(sessionId, "Datafile", dfids.get(0));
					}
					if (dsids.size() != 0) {
						icat.get(sessionId, "Dataset", dsids.get(0));
					}
					if (invids.size() != 0) {
						icat.get(sessionId, "Investigation", invids.get(0));
					}
				} catch (IcatException_Exception e) {
					throw new NotFoundException(e.getMessage());
				}
			}
		} else {
			logger.debug("Slow computation for normal case");
			final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
					datafileIds, Returns.DATASETS_AND_DATAFILES);

			StringBuilder sb = new StringBuilder();
			int n = 0;
			for (DfInfoImpl df : dataSelection.getDfInfo()) {
				if (sb.length() != 0) {
					sb.append(',');
				}
				sb.append(df.getDfId());
				if (n++ == 500) {
					size += getSizeFor(sessionId, sb);
					sb = new StringBuilder();
					n = 0;
				}
			}
			if (n > 0) {
				size += getSizeFor(sessionId, sb);
			}
		}

		if (logSet.contains(CallType.INFO)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("getSize", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}

		return size;
	}

	private long getSizeFor(String sessionId, List<Long> ids, String where) throws InternalException {

		long size = 0;
		if (ids != null) {

			StringBuilder sb = new StringBuilder();
			int n = 0;
			for (Long id : ids) {
				if (sb.length() != 0) {
					sb.append(',');
				}
				sb.append(id);
				if (n++ == 500) {
					size += evalSizeFor(sessionId, where, sb);
					sb = new StringBuilder();
					n = 0;
				}
			}
			if (n > 0) {
				size += evalSizeFor(sessionId, where, sb);
			}
		}
		return size;
	}

	private long getSizeFor(String sessionId, StringBuilder sb) throws InternalException {
		String query = "SELECT SUM(df.fileSize) from Datafile df WHERE df.id IN (" + sb.toString() + ")";
		try {
			return (Long) icat.search(sessionId, query).get(0);
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} catch (IndexOutOfBoundsException e) {
			return 0L;
		}
	}

	private long evalSizeFor(String sessionId, String where, StringBuilder sb) throws InternalException {
		String query = "SELECT SUM(df.fileSize) from Datafile df WHERE " + where + " IN (" + sb.toString() + ")";
		logger.debug("icat query for size: {}", query);
		try {
			return (Long) icat.search(sessionId, query).get(0);
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} catch (IndexOutOfBoundsException e) {
			return 0L;
		}
	}

	public String getStatus(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info(
				String.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		if (sessionId == null) {
			try {
				sessionId = reader.getSessionId();
			} catch (IcatException_Exception e) {
				throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
			}
		} else {
			validateUUID("sessionId", sessionId);
		}

		// Do it
		Status status = Status.ONLINE;

		try {
			if (storageUnit == StorageUnit.DATASET) {
				DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
						datafileIds, Returns.DATASETS);
				Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();

				Set<DsInfo> restoring = fsm.getDsRestoring();
				Set<DsInfo> maybeOffline = fsm.getDsMaybeOffline();
				Set<Long> emptyDatasets = dataSelection.getEmptyDatasets();
				for (DsInfo dsInfo : dsInfos.values()) {
					fsm.checkFailure(dsInfo.getDsId());
					if (restoring.contains(dsInfo)) {
						status = Status.RESTORING;
					} else if (maybeOffline.contains(dsInfo)) {
						status = Status.ARCHIVED;
						break;
					} else if (!emptyDatasets.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
						status = Status.ARCHIVED;
						break;
					}
				}
			} else if (storageUnit == StorageUnit.DATAFILE) {
				DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
						datafileIds, Returns.DATAFILES);
				Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

				Set<DfInfo> restoring = fsm.getDfRestoring();
				Set<DfInfo> maybeOffline = fsm.getDfMaybeOffline();
				for (DfInfo dfInfo : dfInfos) {
					fsm.checkFailure(dfInfo.getDfId());
					if (restoring.contains(dfInfo)) {
						status = Status.RESTORING;
					} else if (maybeOffline.contains(dfInfo)) {
						status = Status.ARCHIVED;
						break;
					} else if (!mainStorage.exists(dfInfo.getDfLocation())) {
						status = Status.ARCHIVED;
						break;
					}
				}
			} else {
				// Throw exception if selection does not exist
				new DataSelection(icat, sessionId, investigationIds, datasetIds, datafileIds, Returns.DATASETS);
			}
		} catch (IOException e) {
			logger.error("I/O Exception " + e.getMessage() + " thrown");
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		logger.debug("Status is " + status.name());

		if (logSet.contains(CallType.INFO)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					if (sessionId != null) {
						gen.write("userName", icat.getUserName(sessionId));
					}
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("getStatus", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}

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
				archiveStorage = propertyHandler.getArchiveStorage();
				twoLevel = archiveStorage != null;
				datatypeFactory = DatatypeFactory.newInstance();
				preparedDir = propertyHandler.getCacheDir().resolve("prepared");
				Files.createDirectories(preparedDir);
				linkDir = propertyHandler.getCacheDir().resolve("link");
				Files.createDirectories(linkDir);

				rootUserNames = propertyHandler.getRootUserNames();
				readOnly = propertyHandler.getReadOnly();

				icat = propertyHandler.getIcatService();

				if (!inited) {
					key = propertyHandler.getKey();
					logger.info("Key is " + (key == null ? "not set" : "set"));
				}

				if (twoLevel) {
					storageUnit = propertyHandler.getStorageUnit();
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

				linkEnabled = propertyHandler.getLinkLifetimeMillis() > 0;

				maxIdsInQuery = propertyHandler.getMaxIdsInQuery();

				threadPool = Executors.newCachedThreadPool();

				logSet = propertyHandler.getLogSet();

				inited = true;

				logger.info("created IdsBean");
			}
		} catch (Throwable e) {
			logger.error("Won't start ", e);
			throw new RuntimeException("IdsBean reports " + e.getClass() + " " + e.getMessage());
		}
	}

	public Boolean isPrepared(String preparedId, String ip)
			throws BadRequestException, NotFoundException, InternalException {

		long start = System.currentTimeMillis();

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

		// TODO Uncomment next line and delete subsequent five lines
		// PreparedStatus status = preparedStatusMap.computeIfAbsent(preparedId,
		// k -> new PreparedStatus());
		PreparedStatus nps = new PreparedStatus();
		PreparedStatus status = preparedStatusMap.putIfAbsent(preparedId, nps);
		if (status == null) {
			status = preparedStatusMap.get(preparedId);
		}

		if (!status.lock.tryLock()) {
			logger.debug("Lock held for evaluation of isPrepared for preparedId {}", preparedId);
			return false;
		}
		try {
			Future<?> future = status.future;
			if (future != null) {
				if (future.isDone()) {
					try {
						future.get();
					} catch (ExecutionException e) {
						throw new InternalException(e.getClass() + " " + e.getMessage());
					} catch (InterruptedException e) {
						// Ignore
					} finally {
						status.future = null;
					}
				} else {
					logger.debug("Background process still running for preparedId {}", preparedId);
					return false;
				}
			}

			try {
				if (storageUnit == StorageUnit.DATASET) {
					Collection<DsInfo> toCheck = status.fromDsElement == null ? preparedJson.dsInfos.values()
							: preparedJson.dsInfos.tailMap(status.fromDsElement).values();
					logger.debug("Will check online status of {} entries", toCheck.size());
					for (DsInfo dsInfo : toCheck) {
						fsm.checkFailure(dsInfo.getDsId());
						if (restoreIfOffline(dsInfo, preparedJson.emptyDatasets)) {
							prepared = false;
							status.fromDsElement = dsInfo.getDsId();
							toCheck = preparedJson.dsInfos.tailMap(status.fromDsElement).values();
							logger.debug("Will check in background status of {} entries", toCheck.size());
							status.future = threadPool.submit(new RunPrepDsCheck(toCheck, preparedJson.emptyDatasets));
							break;
						}
					}
					if (prepared) {
						toCheck = status.fromDsElement == null ? Collections.emptySet()
								: preparedJson.dsInfos.headMap(status.fromDsElement).values();
						logger.debug("Will check finally online status of {} entries", toCheck.size());
						for (DsInfo dsInfo : toCheck) {
							fsm.checkFailure(dsInfo.getDsId());
							if (restoreIfOffline(dsInfo, preparedJson.emptyDatasets)) {
								prepared = false;
							}
						}
					}
				} else if (storageUnit == StorageUnit.DATAFILE) {
					SortedSet<DfInfoImpl> toCheck = status.fromDfElement == null ? preparedJson.dfInfos
							: preparedJson.dfInfos.tailSet(status.fromDfElement);
					logger.debug("Will check online status of {} entries", toCheck.size());
					for (DfInfoImpl dfInfo : toCheck) {
						fsm.checkFailure(dfInfo.getDfId());
						if (restoreIfOffline(dfInfo)) {
							prepared = false;
							status.fromDfElement = dfInfo;
							toCheck = preparedJson.dfInfos.tailSet(status.fromDfElement);
							logger.debug("Will check in background status of {} entries", toCheck.size());
							status.future = threadPool.submit(new RunPrepDfCheck(toCheck));
							break;
						}
					}
					if (prepared) {
						toCheck = status.fromDfElement == null ? new TreeSet<>()
								: preparedJson.dfInfos.headSet(status.fromDfElement);
						logger.debug("Will check finally online status of {} entries", toCheck.size());
						for (DfInfoImpl dfInfo : toCheck) {
							fsm.checkFailure(dfInfo.getDfId());
							if (restoreIfOffline(dfInfo)) {
								prepared = false;
							}
						}
					}
				}
			} catch (IOException e) {
				logger.error("I/O error " + e.getMessage() + " isPrepared of " + preparedId);
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}

			if (logSet.contains(CallType.INFO)) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("preparedId", preparedId);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("isPrepared", ip, body, start);
			}

			return prepared;

		} finally {
			status.lock.unlock();
		}

	}

	public boolean isReadOnly(String ip) {
		if (logSet.contains(CallType.INFO)) {
			transmitter.processMessage("isReadOnly", ip, "{}", System.currentTimeMillis());
		}
		return readOnly;
	}

	public boolean isTwoLevel(String ip) {
		if (logSet.contains(CallType.INFO)) {
			transmitter.processMessage("isTwoLevel", ip, "{}", System.currentTimeMillis());
		}
		return twoLevel;
	}

	public String prepareData(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			boolean compress, boolean zip, String ip)
			throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: prepareData " + "investigationIds='" + investigationIds + "' "
				+ "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "' " + "compress='" + compress
				+ "' " + "zip='" + zip + "'");

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
				datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		String preparedId = UUID.randomUUID().toString();

		Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
		Set<Long> emptyDs = dataSelection.getEmptyDatasets();
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

		if (storageUnit == StorageUnit.DATASET) {
			for (DsInfo dsInfo : dsInfos.values()) {
				fsm.recordSuccess(dsInfo.getDsId());
			}
			threadPool.submit(new RestoreDsTask(dsInfos.values(), emptyDs));

		} else if (storageUnit == StorageUnit.DATAFILE) {
			for (DfInfo dfInfo : dfInfos) {
				fsm.recordSuccess(dfInfo.getDfId());
			}
			threadPool.submit(new RestoreDfTask(dfInfos));
		}

		if (dataSelection.mustZip()) {
			zip = true;
		}

		logger.debug("Writing to " + preparedDir.resolve(preparedId));
		try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(preparedDir.resolve(preparedId)))) {
			pack(stream, zip, compress, dsInfos, dfInfos, emptyDs);
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		logger.debug("preparedId is " + preparedId);

		if (logSet.contains(CallType.PREPARE)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.write("preparedId", preparedId);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("prepareData", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}

		return preparedId;
	}

	public Response put(InputStream body, String sessionId, String name, String datafileFormatIdString,
			String datasetIdString, String description, String doi, String datafileCreateTimeString,
			String datafileModTimeString, boolean wrap, boolean padding, String ip)
			throws NotFoundException, DataNotOnlineException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotImplementedException {

		long start = System.currentTimeMillis();

		try {
			// Log and validate
			logger.info("New webservice request: put " + "name='" + name + "' " + "datafileFormatId='"
					+ datafileFormatIdString + "' " + "datasetId='" + datasetIdString + "' " + "description='"
					+ description + "' " + "doi='" + doi + "' " + "datafileCreateTime='" + datafileCreateTimeString
					+ "' " + "datafileModTime='" + datafileModTimeString + "'");

			if (readOnly) {
				throw new NotImplementedException("This operation has been configured to be unavailable");
			}

			IdsBean.validateUUID("sessionId", sessionId);
			if (name == null) {
				throw new BadRequestException("The name parameter must be set");
			}

			if (datafileFormatIdString == null) {
				throw new BadRequestException("The datafileFormatId parameter must be set");
			}
			long datafileFormatId;
			try {
				datafileFormatId = Long.parseLong(datafileFormatIdString);
			} catch (NumberFormatException e) {
				throw new BadRequestException("The datafileFormatId parameter must be numeric");
			}

			if (datasetIdString == null) {
				throw new BadRequestException("The datasetId parameter must be set");
			}
			long datasetId;
			try {
				datasetId = Long.parseLong(datasetIdString);
			} catch (NumberFormatException e) {
				throw new BadRequestException("The datasetId parameter must be numeric");
			}

			Long datafileCreateTime = null;
			if (datafileCreateTimeString != null) {
				try {
					datafileCreateTime = Long.parseLong(datafileCreateTimeString);
				} catch (NumberFormatException e) {
					throw new BadRequestException("The datafileCreateTime parameter must be numeric");
				}
			}

			Long datafileModTime = null;
			if (datafileModTimeString != null) {
				try {
					datafileModTime = Long.parseLong(datafileModTimeString);
				} catch (NumberFormatException e) {
					throw new BadRequestException("The datafileModTime parameter must be numeric");
				}
			}

			// Do it
			Dataset ds;
			try {
				ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Investigation, Facility", datasetId);
			} catch (IcatException_Exception e) {
				IcatExceptionType type = e.getFaultInfo().getType();
				if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
					throw new InsufficientPrivilegesException(e.getMessage());
				}
				if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
					throw new NotFoundException(e.getMessage());
				}
				throw new InternalException(type + " " + e.getMessage());
			}

			DsInfo dsInfo = new DsInfoImpl(ds);
			String lockId = null;
			if (storageUnit == StorageUnit.DATASET) {
				/*
				 * Lock the datasets to prevent archiving of the datasets. It is
				 * important that they be unlocked again.
				 */
				Set<Long> dsIds = new HashSet<>();
				dsIds.add(datasetId);
				lockId = fsm.lock(dsIds, FiniteStateMachine.SetLockType.ARCHIVE);
				Set<DfInfoImpl> dfInfos = Collections.emptySet();
				Set<Long> emptyDatasets = new HashSet<>();
				try {
					List<Object> counts = icat.search(sessionId,
							"COUNT(Datafile) <-> Dataset [id=" + dsInfo.getDsId() + "]");
					if ((Long) counts.get(0) == 0) {
						emptyDatasets.add(dsInfo.getDsId());
					}
				} catch (IcatException_Exception e) {
					fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE);
					IcatExceptionType type = e.getFaultInfo().getType();
					if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
						throw new InsufficientPrivilegesException(e.getMessage());
					}
					if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
						throw new NotFoundException(e.getMessage());
					}
					throw new InternalException(type + " " + e.getMessage());
				}
				Set<DsInfo> dsInfos = new HashSet<>();
				dsInfos.add(dsInfo);
				checkOnlineAndFreeLockOnFailure(dsInfos, emptyDatasets, dfInfos, lockId,
						FiniteStateMachine.SetLockType.ARCHIVE);
			}

			try {
				CRC32 crc = new CRC32();
				CheckedWithSizeInputStream is = new CheckedWithSizeInputStream(body, crc);
				String location = mainStorage.put(dsInfo, name, is);
				is.close();
				long checksum = crc.getValue();
				long size = is.getSize();
				Long dfId;
				try {
					dfId = registerDatafile(sessionId, name, datafileFormatId, location, checksum, size, ds,
							description, doi, datafileCreateTime, datafileModTime);
				} catch (InsufficientPrivilegesException | NotFoundException | InternalException
						| BadRequestException e) {
					logger.debug("Problem with registration " + e.getClass() + " " + e.getMessage()
							+ " datafile will now be deleted");
					String userId = null;
					try {
						userId = icat.getUserName(sessionId);
					} catch (IcatException_Exception e1) {
						logger.error("Unable to get user name for session " + sessionId + " so mainStorage.delete of "
								+ location + " may fail");
					}
					mainStorage.delete(location, userId, userId);
					throw e;
				}

				if (storageUnit == StorageUnit.DATASET) {
					fsm.queue(dsInfo, DeferredOp.WRITE);
				} else if (storageUnit == StorageUnit.DATAFILE) {
					Datafile df;
					try {
						df = (Datafile) reader.get("Datafile", dfId);
					} catch (IcatException_Exception e) {
						throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
					}
					fsm.queue(new DfInfoImpl(dfId, name, location, df.getCreateId(), df.getModId(), dsInfo.getDsId()),
							DeferredOp.WRITE);
				}

				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				Json.createGenerator(baos).writeStartObject().write("id", dfId).write("checksum", checksum)
						.write("location", location.replace("\\", "\\\\").replace("'", "\\'")).write("size", size)
						.writeEnd().close();
				String resp = wrap ? prefix + baos.toString() + suffix : baos.toString();

				if (logSet.contains(CallType.WRITE)) {
					try {
						baos = new ByteArrayOutputStream();
						try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
							gen.write("userName", icat.getUserName(sessionId));
							gen.write("datafileId", dfId);
							gen.writeEnd();
						}
						transmitter.processMessage("put", ip, baos.toString(), start);
					} catch (IcatException_Exception e) {
						logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
					}
				}

				return Response.status(HttpURLConnection.HTTP_CREATED).entity(resp).build();

			} catch (IOException e) {
				logger.error("I/O exception " + e.getMessage() + " putting " + name + " to Dataset with id "
						+ datasetIdString);
				throw new InternalException(e.getClass() + " " + e.getMessage());
			} finally {
				if (lockId != null) {
					fsm.unlock(lockId, FiniteStateMachine.SetLockType.ARCHIVE);
				}
			}
		} catch (IdsException e) {

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JsonGenerator gen = Json.createGenerator(baos);
			gen.writeStartObject().write("code", e.getClass().getSimpleName()).write("message", e.getShortMessage());
			gen.writeEnd().close();
			if (wrap) {
				String pre = padding ? paddedPrefix : prefix;
				return Response.status(e.getHttpStatusCode()).entity(pre + baos.toString().replace("'", "\\'") + suffix)
						.build();
			} else {
				return Response.status(e.getHttpStatusCode()).entity(baos.toString()).build();
			}
		}

	}

	private Long registerDatafile(String sessionId, String name, long datafileFormatId, String location, long checksum,
			long size, Dataset dataset, String description, String doi, Long datafileCreateTime, Long datafileModTime)
			throws InsufficientPrivilegesException, NotFoundException, InternalException, BadRequestException {
		final Datafile df = new Datafile();
		DatafileFormat format;
		try {
			format = (DatafileFormat) icat.get(sessionId, "DatafileFormat", datafileFormatId);
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
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
			long dfId = icat.create(sessionId, df);
			df.setId(dfId);

			if (key != null) {
				df.setLocation(location + " " + IcatSecurity.digest(dfId, location, key));
				icat.update(sessionId, df);
			}

			logger.debug("Registered datafile for dataset {} for {}", dataset.getId(), name + " at " + location);
			return dfId;
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES || type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.VALIDATION) {
				throw new BadRequestException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		} catch (NoSuchAlgorithmException e) {
			throw new InternalException(e.getMessage());
		}
	}

	public void reset(String preparedId, String ip) throws BadRequestException, InternalException, NotFoundException {
		long start = System.currentTimeMillis();

		logger.info(String.format("New webservice request: reset preparedId=%s", preparedId));

		// Validate
		validateUUID("preparedId", preparedId);

		// Do it
		Prepared preparedJson;
		try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
			preparedJson = unpack(stream);
		} catch (NoSuchFileException e) {
			throw new NotFoundException("The preparedId " + preparedId + " is not known");
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		if (storageUnit == StorageUnit.DATASET) {
			for (DsInfo dsInfo : preparedJson.dsInfos.values()) {
				fsm.recordSuccess(dsInfo.getDsId());
			}
		} else if (storageUnit == StorageUnit.DATAFILE) {
			for (DfInfoImpl dfInfo : preparedJson.dfInfos) {
				fsm.recordSuccess(dfInfo.getDfId());
			}
		}

		if (logSet.contains(CallType.MIGRATE)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.write("preparedId", preparedId);
				gen.writeEnd();
			}
			String body = baos.toString();
			transmitter.processMessage("reset", ip, body, start);
		}
	}

	public void reset(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {
		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: reset " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
				+ datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds,
				datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		if (storageUnit == StorageUnit.DATASET) {
			for (DsInfo dsInfo : dataSelection.getDsInfo().values()) {
				fsm.recordSuccess(dsInfo.getDsId());
			}
		} else if (storageUnit == StorageUnit.DATAFILE) {
			for (DfInfoImpl dfInfo : dataSelection.getDfInfo()) {
				fsm.recordSuccess(dfInfo.getDfId());
			}
		}

		if (logSet.contains(CallType.MIGRATE)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				String body = baos.toString();
				transmitter.processMessage("reset", ip, body, start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}
	}

	private void restartUnfinishedWork() throws InternalException {

		try {
			for (File file : markerDir.toFile().listFiles()) {
				if (storageUnit == StorageUnit.DATASET) {
					long dsid = Long.parseLong(file.toPath().getFileName().toString());
					Dataset ds = null;
					try {
						ds = (Dataset) reader.get("Dataset ds INCLUDE ds.investigation.facility", dsid);
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
				} else if (storageUnit == StorageUnit.DATAFILE) {
					long dfid = Long.parseLong(file.toPath().getFileName().toString());
					Datafile df = null;
					try {
						df = (Datafile) reader.get("Datafile ds INCLUDE ds.dataset", dfid);
						String location = getLocation(df.getId(), df.getLocation());
						DfInfoImpl dfInfo = new DfInfoImpl(dfid, df.getName(), location, df.getCreateId(),
								df.getModId(), df.getDataset().getId());
						fsm.queue(dfInfo, DeferredOp.WRITE);
						logger.info("Queued datafile with id " + dfid + " " + dfInfo
								+ " to be written as it was not written out previously by IDS");
					} catch (IcatException_Exception e) {
						if (e.getFaultInfo().getType() == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
							logger.warn("Datafile with id " + dfid
									+ " was not written out by IDS and now no longer known to ICAT");
							Files.delete(file.toPath());
						} else {
							throw e;
						}
					}
				}
			}
		} catch (Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	public void restore(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws BadRequestException, InsufficientPrivilegesException, InternalException, NotFoundException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: restore " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
				+ datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		validateUUID("sessionId", sessionId);

		// Do it
		if (storageUnit == StorageUnit.DATASET) {
			DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds, datafileIds,
					Returns.DATASETS);
			Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos.values()) {
				fsm.queue(dsInfo, DeferredOp.RESTORE);
			}
		} else if (storageUnit == StorageUnit.DATAFILE) {
			DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds, datasetIds, datafileIds,
					Returns.DATAFILES);
			Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();
			for (DfInfoImpl dfInfo : dfInfos) {
				fsm.queue(dfInfo, DeferredOp.RESTORE);
			}
		}

		if (logSet.contains(CallType.MIGRATE)) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
					gen.write("userName", icat.getUserName(sessionId));
					addIds(gen, investigationIds, datasetIds, datafileIds);
					gen.writeEnd();
				}
				transmitter.processMessage("restore", ip, baos.toString(), start);
			} catch (IcatException_Exception e) {
				logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
			}
		}
	}

	private boolean restoreIfOffline(DfInfoImpl dfInfo) throws InternalException, IOException {
		boolean maybeOffline = false;
		if (fsm.getDfMaybeOffline().contains(dfInfo)) {
			maybeOffline = true;
		} else if (!mainStorage.exists(dfInfo.getDfLocation())) {
			fsm.queue(dfInfo, DeferredOp.RESTORE);
			maybeOffline = true;
		}
		return maybeOffline;
	}

	private boolean restoreIfOffline(DsInfo dsInfo, Set<Long> emptyDatasets) throws InternalException, IOException {
		boolean maybeOffline = false;
		if (fsm.getDsMaybeOffline().contains(dsInfo)) {
			maybeOffline = true;
		} else if (!emptyDatasets.contains(dsInfo.getDsId()) && !mainStorage.exists(dsInfo)) {
			fsm.queue(dsInfo, DeferredOp.RESTORE);
			maybeOffline = true;
		}
		return maybeOffline;
	}
}
