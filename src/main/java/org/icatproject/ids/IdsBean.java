package org.icatproject.ids;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;

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
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.thread.RestorerThreadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
public class IdsBean {

	private static final Logger logger = LoggerFactory.getLogger(IdsBean.class);

	// matches standard UUID format of 8-4-4-4-12 hexadecimal digits
	public static final Pattern uuidRegExp = Pattern
			.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

	private static Boolean inited = false;

	private static AtomicLong atomicLong = new AtomicLong();

	@EJB
	private RestorerThreadManager restorerThreadManager;

	@EJB
	IcatReader reader;

	private MainStorageInterface mainStorage;

	private PropertyHandler propertyHandler;

	private ICAT icat;

	private Set<String> rootUserNames;

	private PreparedFilesManager preparedFilesManager;

	private FailedFilesManager failedFilesManager;

	private RestoreFileCountManager restoreFileCountManager;


	@PostConstruct
	private void init() {
		try {
			synchronized (inited) {
				logger.info("creating IdsBean");
				propertyHandler = PropertyHandler.getInstance();
				mainStorage = propertyHandler.getMainStorage();
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

				icat = propertyHandler.getIcatService();

				if (!inited) {
					restartUnfinishedWork();
					cleanPreparedDir(preparedDir);
				}
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

		logger.info("New webservice request: prepareData " + 
				"investigationIds='{}' datasetIds='{}' datafileIds='{}' " + 
				"compress='{}' zip='{}'",
				new Object[] {investigationIds, datasetIds, datafileIds, compress, zip});

		validateUUID("sessionId", sessionId);

		// retain lookup of Dataset information (as well as Datafile: DATASETS_AND_DATAFILES) 
		// for now to keep prepared files compatible with the IcatProject IDS
		final DataSelection dataSelection = new DataSelection(propertyHandler, reader, sessionId,
				investigationIds, datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		Map<Long, DsInfo> dsInfos = dataSelection.getDsInfo();
		Set<Long> emptyDs = dataSelection.getEmptyDatasets();
		Set<DfInfoImpl> dfInfos = dataSelection.getDfInfo();

		String preparedId = UUID.randomUUID().toString();
		logger.debug("preparedId is {}", preparedId);

		preparedFilesManager.pack(preparedId, zip, compress, dsInfos, dfInfos, emptyDs);

		restoreFileCountManager.addEntryToMap(preparedId, dfInfos.size());

		restorerThreadManager.submitFilesForRestore(preparedId, new ArrayList<>(dfInfos), true);

		return preparedId;
	}

	public Boolean isPrepared(String preparedId, String ip)
			throws BadRequestException, NotFoundException, InternalException {

		logger.info("New webservice request: isPrepared preparedId = {}", preparedId);

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

		return isPrepared;
	}

	public String getPercentageComplete(String preparedId, String remoteAddr) throws InternalException, NotFoundException {
		logger.info("New webservice request: getPercentageComplete preparedId = {}", preparedId);
		return restorerThreadManager.getPercentageComplete(preparedId);
	}

	public void cancel(String preparedId, String remoteAddr) throws NotFoundException {
		logger.info("New webservice request: cancel preparedId = {}", preparedId);
		restorerThreadManager.cancelThreadsForPreparedId(preparedId);
	}

	public String getDatafileIds(String preparedId, String ip)
			throws BadRequestException, InternalException, NotFoundException {

		// Log and validate
		logger.info("New webservice request: getDatafileIds preparedId = {}", preparedId);

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
		return baos.toString();
	}

	public Response getData(String preparedId, String outname, final long offset, String ip) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException {

		logger.info("New webservice request: getData preparedId = {} outname = {} offset = {}", 
				new Object[] {preparedId, outname, offset});

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

		Long transferId = atomicLong.getAndIncrement();

		return Response.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(new IdsStreamingOutput(dsInfos, dfInfos, offset, zip, compress, transferId, ip, time))
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"").header("Accept-Ranges", "bytes")
				.build();
	}

	public long getSize(String preparedId, String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

		// Log and validate
		logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
		validateUUID("preparedId", preparedId);

		// Do it
		Prepared prepared = preparedFilesManager.unpack(preparedId);

		final Set<DfInfoImpl> dfInfos = prepared.dfInfos;

		// Note that the "fast computation for the simple case" 
		// (see the other getSize() implementation) is not
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

		return size;
	}

	public long getSize(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
			throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

		// Log and validate
		logger.info("New webservice request: getSize investigationIds={}, datasetIds={}, datafileIds={}",
				new Object[] {investigationIds, datasetIds, datafileIds});

		if (propertyHandler.getUseReaderForPerformance()) {
			try {
				sessionId = reader.getSessionId();
			} catch (IcatException_Exception e) {
				throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
			}
		}
		
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

		// TODO: implement something that gives an overview of all restore threads?
		// is it possible to also track what downloads are in progress?
		// where is this information currently displayed? - in a browser? topcat admin page/script?
		return restorerThreadManager.getStatusString();
	}

	public String getIcatUrl(String ip) {
		return propertyHandler.getIcatUrl();
	}

	// TODO: should this method remove the 'failed' file for a prepared ID?
	// (this method previously removed items from the set of 'failures' so that 
	// restoration could be re-attempted without skipping the 'failures')
	public void reset(String preparedId, String ip) throws BadRequestException, InternalException, NotFoundException {
		logger.info(String.format("New webservice request: reset preparedId=%s", preparedId));

		validateUUID("preparedId", preparedId);

		// TODO: remove the 'failed' file?
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
					logger.debug("Deleted {} to reclaim {} bytes", path, thisSize);
				} catch (IOException e) {
					logger.debug("Failed to delete {} : {}", path, e.getMessage());
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

}
