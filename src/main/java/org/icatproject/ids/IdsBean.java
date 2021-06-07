package org.icatproject.ids;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.icatproject.Datafile;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.DataSelection.Returns;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.icatproject.ids.thread.RestorerThreadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
public class IdsBean {

	private final static Logger logger = LoggerFactory.getLogger(IdsBean.class);

	private static final int BUFSIZ = 2048;

	// matches standard UUID format of 8-4-4-4-12 hexadecimal digits
	public static final Pattern uuidRegExp = Pattern
			.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

	private static Boolean inited = false;

	private static AtomicLong atomicLong = new AtomicLong();

	@EJB
	private RestorerThreadManager restorerThreadManager;

	@EJB
	IcatReader reader;

	@EJB
	Transmitter transmitter;

	private MainStorageInterface mainStorage;

	private ZipMapperInterface zipMapper;

	private PropertyHandler propertyHandler;

	private ICAT icat;

	private Set<String> rootUserNames;

	private PreparedFilesManager preparedFilesManager;

	private FailedFilesManager failedFilesManager;

	private RestoreFileCountManager restoreFileCountManager;

	private boolean readOnly;

	private boolean twoLevel;

	private Set<CallType> logSet;

	enum CallType {
		INFO, PREPARE, READ, WRITE, MIGRATE, LINK
	};

	@PostConstruct
	private void init() {
		try {
			synchronized (inited) {
				logger.info("creating IdsBean");
				propertyHandler = PropertyHandler.getInstance();
				zipMapper = propertyHandler.getZipMapper();
				mainStorage = propertyHandler.getMainStorage();
				twoLevel = propertyHandler.getArchiveStorageClass() != null;
				Path preparedDir = propertyHandler.getCacheDir().resolve(Constants.PREPARED_DIR_NAME);
				Files.createDirectories(preparedDir);
				preparedFilesManager = new PreparedFilesManager(preparedDir);
				Path completedDir = propertyHandler.getCacheDir().resolve(Constants.COMPLETED_DIR_NAME);
				Files.createDirectories(completedDir);
				Path failedFilesDir = propertyHandler.getCacheDir().resolve(Constants.FAILED_DIR_NAME);
				Files.createDirectories(failedFilesDir);
				failedFilesManager = new FailedFilesManager(propertyHandler.getCacheDir());
				restoreFileCountManager = RestoreFileCountManager.getInstance();

				rootUserNames = propertyHandler.getRootUserNames();
				readOnly = propertyHandler.getReadOnly();

				icat = propertyHandler.getIcatService();

				if (twoLevel) {
					if (!inited) {
						restartUnfinishedWork();
					}
				}

				if (!inited) {
					cleanPreparedDir(preparedDir);
				}

				logSet = propertyHandler.getLogSet();

				inited = true;

				logger.info("created IdsBean");
			}
		} catch (Throwable e) {
			logger.error("Won't start ", e);
			throw new RuntimeException("IdsBean reports " + e.getClass() + " " + e.getMessage());
		}
	}

	public String prepareData(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			boolean compress, boolean zip, String ip)
			throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException {

		long start = System.currentTimeMillis();

		logger.info("New webservice request: prepareData " + "investigationIds='" + investigationIds + "' "
				+ "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "' " + "compress='" + compress
				+ "' " + "zip='" + zip + "'");

		validateUUID("sessionId", sessionId);

		final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
				investigationIds, datasetIds, datafileIds, Returns.DATAFILES);

		Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
		Set<Long> emptyDs = dataSelection.getEmptyDatasets();
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

		String preparedId = UUID.randomUUID().toString();
		logger.debug("preparedId is {}", preparedId);

		preparedFilesManager.pack(preparedId, zip, compress, dsInfos, dfInfos, emptyDs);

		restoreFileCountManager.addEntryToMap(preparedId, dfInfos.size());

		restorerThreadManager.submitFilesForRestore(preparedId, new ArrayList<>(dfInfos), true);

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

	public Boolean isPrepared(String preparedId, String ip)
			throws BadRequestException, NotFoundException, InternalException {

		long start = System.currentTimeMillis();

		logger.info("New webservice request: isPrepared preparedId={}", preparedId);

		validateUUID("preparedId", preparedId);

		boolean isPrepared = false;

		if (restorerThreadManager.getTotalNumFilesRemaining(preparedId) == 0) {
			// there are no restore threads running for this prepared ID
			// check whether all the files are still on the cache
			Prepared prepared = preparedFilesManager.unpack(preparedId);
			Set<String> failedFiles = failedFilesManager.getFailedEntriesForPreparedId(preparedId);
			List<DfInfo> dfInfosStillMissing = checkFilesOnline(prepared.dfInfos, failedFiles);
			if (dfInfosStillMissing.isEmpty()) {
				// all files were either on the cache or 
				// in the list of files that failed to restore
				isPrepared = true;
			} else {
				// restore the files that were missing
				restorerThreadManager.submitFilesForRestore(preparedId, dfInfosStillMissing, false);
			}
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

		return isPrepared;
	}

	public String getDatafileIds(String preparedId, String ip)
			throws BadRequestException, InternalException, NotFoundException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: getDatafileIds preparedId = '" + preparedId);

		validateUUID("preparedId", preparedId);

		// Do it
		Prepared prepared = preparedFilesManager.unpack(preparedId);

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

	public Response getData(String preparedId, String outname, final long offset, String ip) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException {

		long time = System.currentTimeMillis();

		logger.info("New webservice request: getData preparedId = '" + preparedId + "' outname = '" + outname
				+ "' offset = " + offset);

		validateUUID("preparedId", preparedId);

		boolean isPrepared = false;
		// these will be needed later on if the data is all online
		Prepared prepared = null;
		Set<String> failedFiles = null;

		String restoreStatus = "in progress";
		if (restorerThreadManager.getTotalNumFilesRemaining(preparedId) == 0) {
			// there are no restore threads running for this prepared ID
			// check whether all the files are still on the cache
			prepared = preparedFilesManager.unpack(preparedId);
			failedFiles = failedFilesManager.getFailedEntriesForPreparedId(preparedId);
			List<DfInfo> dfInfosStillMissing = checkFilesOnline(prepared.dfInfos, failedFiles);
			if (dfInfosStillMissing.isEmpty()) {
				// all files were either on the cache or 
				// in the list of files that failed to restore
				isPrepared = true;
			} else {
				// restore the files that were missing
				restorerThreadManager.submitFilesForRestore(preparedId, dfInfosStillMissing, false);
				restoreStatus = "requested";
			}
		}

		if (!isPrepared) {
			String message = "Not all files are online for prepared ID " + preparedId + " - Restore " + restoreStatus;
			logger.info(message);
			throw new DataNotOnlineException(message);
		}

		final boolean zip = prepared.zip;
		final boolean compress = prepared.compress;
		final Set<DfInfoImpl> dfInfos = prepared.dfInfos;
		final Map<Long, DsInfo> dsInfos = prepared.dsInfos;

		// construct the name to include in the headers 
		String name;
		if (outname == null) {
			if (prepared.zip) {
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
				// TODO: pass the failedFiles so that they can be skipped in the zip file
				.entity(new SO(dsInfos, dfInfos, offset, zip, compress, transferId, ip, time))
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"").header("Accept-Ranges", "bytes")
				.build();
	}

	public long getSize(String preparedId, String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

		long start = System.currentTimeMillis();

		// Log and validate
		logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
		validateUUID("preparedId", preparedId);

		// Do it
		Prepared prepared = preparedFilesManager.unpack(preparedId);

		final Set<DfInfoImpl> dfInfos = prepared.dfInfos;

		// Note that the "fast computation for the simple case" (see the other getSize() implementation) is not
		// available when calling getSize() with a preparedId.
		logger.debug("Slow computation for normal case");
		String sessionId;
		try {
			sessionId = reader.getSessionId();
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
		}
		long size = 0;

		StringBuilder sb = new StringBuilder();
		int n = 0;
		for (DfInfoImpl df : dfInfos) {
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

		if (logSet.contains(CallType.INFO)) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.write("preparedId", preparedId);
				gen.writeEnd();
			}
			String body = baos.toString();
			transmitter.processMessage("getSize", ip, body, start);
		}

		return size;
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
						Datafile datafile = (Datafile) icat.get(sessionId, "Datafile", dfids.get(0));
						if (datafile.getLocation() == null) {
							throw new NotFoundException("Datafile not found");
						}
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
			final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
					investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

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
		String query = "SELECT SUM(df.fileSize) from Datafile df WHERE df.id IN (" + sb.toString() + ") AND df.location IS NOT NULL";
		try {
			return (Long) icat.search(sessionId, query).get(0);
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} catch (IndexOutOfBoundsException e) {
			return 0L;
		}
	}

	private long evalSizeFor(String sessionId, String where, StringBuilder sb) throws InternalException {
		String query = "SELECT SUM(df.fileSize) from Datafile df WHERE " + where + " IN (" + sb.toString() + ") AND df.location IS NOT NULL";
		logger.debug("icat query for size: {}", query);
		try {
			return (Long) icat.search(sessionId, query).get(0);
		} catch (IcatException_Exception e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		} catch (IndexOutOfBoundsException e) {
			return 0L;
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

		// TODO: implement something that gives an overview of all restore threads?
		// is it possible to also track what downloads are in progress?
		// where is this information currently displayed? - in a browser? topcat admin page/script?
		return restorerThreadManager.getStatusString();
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

	public String getIcatUrl(String ip) {
		if (logSet.contains(CallType.INFO)) {
			transmitter.processMessage("getIcatUrl", ip, "{}", System.currentTimeMillis());
		}
		return propertyHandler.getIcatUrl();
	}

	// TODO: should this method remove the 'failed' file for a prepared ID?
	// (this method previously removed items from the set of 'failures' so that 
	// restoration could be re-attempted without skipping the 'failures')
	public void reset(String preparedId, String ip) throws BadRequestException, InternalException, NotFoundException {
		long start = System.currentTimeMillis();

		logger.info(String.format("New webservice request: reset preparedId=%s", preparedId));

		validateUUID("preparedId", preparedId);

		// TODO: remove the 'failed' file?

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

	// used by the JMS message sending to add IDs to the message
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

	/**
	 * Check that all of the files are on the cache apart from any that are
	 * listed in the set of failed files.
	 * 
	 * @param dfInfos the set of DatafileInfo objects to check
	 * @param failedFiles a list of files that failed to restore and therefore
	 *                    will not be on the cache
	 * @return a list of DatafileInfo objects for any files that were found to
	 *         be missing (and not in the list of failed files)
	 */
	private List<DfInfo> checkFilesOnline(SortedSet<DfInfoImpl> dfInfos, Set<String> failedFiles) {
		List<DfInfo> dfInfosStillMissing = new ArrayList<>();
		for (DfInfo dfInfo: dfInfos) {
			if (!mainStorage.exists(dfInfo.getDfLocation())) {
				// this file is not on the cache
				if (!failedFiles.contains(dfInfo.getDfLocation())) {
					// and it is not one that failed to restore
					// Add it to a list to re-request.
					dfInfosStillMissing.add(dfInfo);
				}
			}
		}
		return dfInfosStillMissing;
	}

	private void restartUnfinishedWork() throws InternalException {
		// TODO: does this need re-implementing?
		// how to make it re-start any unfinished restore requests when an IDS is restarted?
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
			throw new InternalException("location is null");
		}
		return location;
	}

	public static void validateUUID(String thing, String id) throws BadRequestException {
		if (id == null || !uuidRegExp.matcher(id).matches())
			throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
	}

	// TODO: move this out into a separate class with a better name!
	private class SO implements StreamingOutput {

		private long offset;
		private boolean zip;
		private Map<Long, DsInfo> dsInfos;
		private boolean compress;
		private Set<DfInfoImpl> dfInfos;
		private String ip;
		private long start;
		private Long transferId;

		SO(Map<Long, DsInfo> dsInfos, Set<DfInfoImpl> dfInfos, long offset, 
				boolean zip, boolean compress, Long transferId, String ip, long start) {
			this.offset = offset;
			this.zip = zip;
			this.dsInfos = dsInfos;
			this.dfInfos = dfInfos;
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
			}
		}

	}

	// Below are the methods that are no longer available in the DLS IDS 
	// because they are not used. For now they log that they are being called 
	// as an error and throw a NotImplementedException so that hopefully what 
	// is calling them can be investigated to ensure it is not required.
	// TODO: decide whether to remove them completely in the final version 
	// along with the corresponding method in IdsService. I believe a generic
	// NotImplementedException will be thrown in this case anyway - check.

	public void archive(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
	    	throws NotImplementedException {
		logger.error("New webservice request: archive (NOT AVAILABLE) " + "investigationIds='" + investigationIds + 
				"' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public void delete(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: delete (NOT AVAILABLE) " + "investigationIds='" + investigationIds + 
				"' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public Response getData(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			final boolean compress, boolean zip, String outname, final long offset, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: getData (NOT AVAILABLE) " + "investigationIds='" + investigationIds + 
				"' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public String getDatafileIds(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			String ip)
			throws NotImplementedException {
		logger.error("New webservice request: getDatafileIds (NOT AVAILABLE) " + "investigationIds='" + investigationIds + 
				"' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public Response put(InputStream body, String sessionId, String name, String datafileFormatIdString,
			String datasetIdString, String description, String doi, String datafileCreateTimeString,
			String datafileModTimeString, boolean wrap, boolean padding, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: put (NOT AVAILABLE) " + "name='" + name + "' " + "datafileFormatId='"
				+ datafileFormatIdString + "' " + "datasetId='" + datasetIdString + "' " + "description='"
				+ description + "' " + "doi='" + doi + "' " + "datafileCreateTime='" + datafileCreateTimeString
				+ "' " + "datafileModTime='" + datafileModTimeString + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public String getLink(String sessionId, long datafileId, String username, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: getLink (NOT AVAILABLE) datafileId=" + datafileId + 
				" username='" + username + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public String getStatus(String preparedId, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: getStatus (NOT AVAILABLE) preparedId = '{}'", preparedId);
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public String getStatus(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: getStatus (NOT AVAILABLE) " + "investigationIds='" + investigationIds 
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public void restore(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
		throws NotImplementedException {
		logger.error("New webservice request: restore (NOT AVAILABLE) " + "investigationIds='" + investigationIds 
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public void write(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: write (NOT AVAILABLE) " + "investigationIds='" + investigationIds 
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

	public void reset(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws NotImplementedException {
		logger.error("New webservice request: reset (NOT AVAILABLE) " + "investigationIds='" + investigationIds 
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");
		throw new NotImplementedException("This operation is not available in this IDS");
	}

}
