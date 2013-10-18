package org.icatproject.ids.webservice;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.persistence.PersistenceException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.entity.IdsRequestEntity;
import org.icatproject.ids.plugin.StorageInterface;
import org.icatproject.ids.thread.ProcessQueue;
import org.icatproject.ids.util.Icat;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.RequestHelper;
import org.icatproject.ids.util.RequestQueues;
import org.icatproject.ids.util.RequestedState;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.util.ValidationHelper;
import org.icatproject.ids.util.ZipHelper;
import org.icatproject.ids.webservice.exceptions.BadRequestException;
import org.icatproject.ids.webservice.exceptions.ForbiddenException;
import org.icatproject.ids.webservice.exceptions.IdsException;
import org.icatproject.ids.webservice.exceptions.IdsException.Code;
import org.icatproject.ids.webservice.exceptions.InternalServerErrorException;
import org.icatproject.ids.webservice.exceptions.NotFoundException;
import org.icatproject.ids.webservice.exceptions.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the IDS specification for the IDS Reference Implementation.
 */
@Path("/")
@Stateless
// @TransactionAttribute(TransactionAttributeType.NEVER)
public class IdsService {

	private final static Logger logger = LoggerFactory.getLogger(IdsService.class);

	private long archiveWriteDelayMillis = PropertyHandler.getInstance().getWriteDelaySeconds() * 1000L;
	private Timer timer = new Timer();
	private RequestQueues requestQueues = RequestQueues.getInstance();

	private static final int BUFSIZ = 2048;

	@EJB
	private Icat icatClient;

	@EJB
	private RequestHelper requestHelper;

	private StorageInterface fastStorage;

	@PostConstruct
	public void postConstructInit() throws Exception {
		logger.info("creating WebService");
		PropertyHandler ph = PropertyHandler.getInstance();
		fastStorage = ph.getMainStorage();

		timer.schedule(new ProcessQueue(timer, requestHelper),
				ph.getProcessQueueIntervalSeconds() * 1000L);
		restartUnfinishedWork();
		logger.info("created WebService");
	}

	/**
	 * Creates a new download request. Does not accept investigationIds or the zip parameter as all
	 * requested files will always be returned in a zip file.
	 * 
	 * @param sessionId
	 *            An ICAT session ID
	 * @param investigationIds
	 *            A list of investigation IDs (not implemented)
	 * @param datasetIds
	 *            A list of dataset IDs (optional)
	 * @param datafileIds
	 *            A list of datafile IDs (optional)
	 * @param compress
	 *            Compress ZIP archive of files (dependent on ZIP parameter below) (optional)
	 * @param zip
	 *            Request data to be packaged in a ZIP archive (not implemented)
	 * @return HTTP response containing a preparedId for the download request
	 * @throws NotImplementedException
	 * @throws BadRequestException
	 * @throws ForbiddenException
	 * @throws NotFoundException
	 * @throws InternalServerErrorException
	 */
	@POST
	@Path("prepareData")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response prepareData(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds,
			@FormParam("datafileIds") String datafileIds,
			@DefaultValue("false") @FormParam("compress") boolean compress,
			@FormParam("zip") String zip) throws NotImplementedException, BadRequestException,
			ForbiddenException, NotFoundException, InternalServerErrorException {
		IdsRequestEntity requestEntity = null;
		logger.info("prepareData received");

		if (investigationIds != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"investigationIds are not supported");
		}
		if (zip != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"the zip parameter is not supported");
		}
		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(Code.BAD_SESSION_ID, "The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException(Code.BAD_DATASET_IDS,
					"The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException(Code.BAD_DATAFILE_IDS,
					"The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"At least one of datasetIds or datafileIds parameters must be set");
		}

		// at this point we're sure, that all arguments are valid
		logger.info("New webservice request: prepareData " + "investigationIds='"
				+ investigationIds + "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='"
				+ datafileIds + "' " + "compress='" + compress + "' " + "zip='" + zip + "'");

		try {
			requestEntity = requestHelper.createPrepareRequest(sessionId, compress, zip);
			if (datafileIds != null) {
				requestHelper.addDatafiles(sessionId, requestEntity, datafileIds);
			}
			if (datasetIds != null) {
				requestHelper.addDatasets(sessionId, requestEntity, datasetIds);
			}
			for (IdsDataEntity de : requestEntity.getDataEntities()) {
				this.queue(de, DeferredOp.PREPARE);
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			switch (type) {
			case SESSION:
				throw new ForbiddenException(Code.BAD_SESSION_ID,
						"The sessionId parameter is invalid or has expired");
			case INSUFFICIENT_PRIVILEGES:
				requestHelper.setRequestStatus(requestEntity, StatusInfo.ERROR);
				throw new ForbiddenException(Code.DENIED,
						"You don't have sufficient privileges to perform this operation");
			case NO_SUCH_OBJECT_FOUND:
				requestHelper.setRequestStatus(requestEntity, StatusInfo.NOT_FOUND);
				throw new NotFoundException(Code.NOT_IN_ICAT, "Could not find requested objects");
			case INTERNAL:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
						"Unable to connect to ICAT server");
			default:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
						"Unrecognized ICAT exception " + e);
			}
		} catch (PersistenceException e) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
					"Unable to connect to the database");
		} catch (Exception e) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, t.getMessage());
		}
		return Response.status(200).entity(requestEntity.getPreparedId() + "\n").build();
	}

	/**
	 * This method is specifically tailored to the IDS. It will remove files from the cache that
	 * match the userId and any of the dataset or datafile ids.
	 * 
	 * TODO: try doing better queries -> join with DOWNLOAD_REQUEST and check for username
	 * 
	 * TODO: throw FileNotFoundException if no matching requests are found !
	 * 
	 * @param sessionId
	 *            An ICAT session ID
	 * @param investigationIds
	 *            A list of investigation IDs (not implemented)
	 * @param datasetIds
	 *            A list of dataset IDs (optional)
	 * @param datafileIds
	 *            A list of datafile IDs (optional)
	 * @return Empty response
	 * @throws BadRequestException
	 * @throws ForbiddenException
	 * @throws NotImplementedException
	 * @throws InternalServerErrorException
	 */
	@POST
	@Path("archive")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response archive(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws BadRequestException, ForbiddenException, NotImplementedException,
			InternalServerErrorException {
		IdsRequestEntity requestEntity = null;

		// 501
		if (investigationIds != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"investigationIds are not supported");
		}
		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(Code.BAD_SESSION_ID, "The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException(Code.BAD_DATASET_IDS,
					"The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException(Code.BAD_DATAFILE_IDS,
					"The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"At least one of datasetIds or datafileIds parameters must be set");
		}

		logger.info("New webservice request: archive " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		try {
			requestEntity = requestHelper.createArchiveRequest(sessionId);
			if (datafileIds != null) {
				requestHelper.addDatafiles(sessionId, requestEntity, datafileIds);
			}
			if (datasetIds != null) {
				requestHelper.addDatasets(sessionId, requestEntity, datasetIds);
			}
			for (IdsDataEntity de : requestEntity.getDataEntities()) {
				this.queue(de, DeferredOp.ARCHIVE);
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			switch (type) {
			case SESSION:
				throw new ForbiddenException(Code.BAD_SESSION_ID,
						"The sessionId parameter is invalid or has expired");
			default:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
						"Unrecognized ICAT exception " + e);
			}
		} catch (PersistenceException e) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
					"Unable to connect to the database");
		} catch (Exception e) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, t.getMessage());
		}

		return Response.status(200).build();
	}

	@GET
	@Path("getStatus")
	@Produces("text/plain")
	public Response getStatus(@QueryParam("preparedId") String preparedId,
			@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafilesIds) throws BadRequestException,
			NotFoundException, InternalServerErrorException, ForbiddenException,
			NotImplementedException {
		Response status = null;
		logger.info("received getStatus with preparedId = " + preparedId);
		if (preparedId != null) {
			status = getStatus(preparedId);
		} else {
			status = getStatus(sessionId, investigationIds, datasetIds, datafilesIds);
		}
		return status;
	}

	/**
	 * Returns the current status of a download request. The current status values that can be
	 * returned are ONLINE and RESTORING.
	 * 
	 * TODO: determine if database connection lost TODO: check if INCOMPLETE status should be
	 * implemented
	 * 
	 * @param preparedId
	 *            The ID of the download request
	 * @return HTTP response containing the current status of the download request (ONLINE,
	 *         IMCOMPLETE, RESTORING, ARCHIVED) Note: only ONELINE and RESTORING are implemented
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalServerErrorException
	 * @throws ForbiddenException
	 */
	private Response getStatus(String preparedId) throws BadRequestException, NotFoundException,
			InternalServerErrorException, ForbiddenException {
		String status = null;

		if (ValidationHelper.isValidId(preparedId) == false) {
			throw new BadRequestException(Code.BAD_PREPARED_ID,
					"The preparedId parameter is invalid");
		}

		List<IdsRequestEntity> requests = requestHelper.getRequestByPreparedId(preparedId);
		if (requests.size() == 0) {
			throw new NotFoundException(Code.NO_PREPARED_ID, "No matches found for preparedId \""
					+ preparedId + "\"");
		}
		if (requests.size() > 1) {
			String msg = "More than one match found for preparedId \"" + preparedId + "\"";
			logger.error(msg);
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, msg);
		}
		status = requests.get(0).getStatus().name();

		logger.info("New webservice request: getStatus " + "preparedId='" + preparedId + "'");

		// convert internal status to appropriate external status
		StatusInfo statusinfo = StatusInfo.valueOf(status);
		if (statusinfo == StatusInfo.COMPLETED) {
			status = Status.ONLINE.name();
		} else if (statusinfo == StatusInfo.INCOMPLETE) {
			status = Status.INCOMPLETE.name();
		} else if (statusinfo == StatusInfo.DENIED) {
			throw new ForbiddenException(Code.DENIED,
					"You do not have permission to download one or more of the requested files");
		} else if (statusinfo == StatusInfo.NOT_FOUND) {
			throw new NotFoundException(Code.NOT_IN_ICAT,
					"Some of the requested datafile / dataset ids were not found");
		} else if (statusinfo == StatusInfo.ERROR) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
					"Unable to find files in storage");
		} else {
			status = Status.RESTORING.name();
		}

		return Response.status(200).entity(status + "\n").build();
	}

	private Response getStatus(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			ForbiddenException, NotFoundException, InternalServerErrorException {
		// 501
		if (investigationIds != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"investigationIds are not supported");
		}
		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(IdsException.Code.BAD_SESSION_ID,
					"The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException(Code.BAD_DATASET_IDS,
					"The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException(Code.BAD_DATAFILE_IDS,
					"The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"At least one of datasetIds or datafileIds parameters must be set");
		}

		logger.info(String
				.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		// assuming everything is available
		Status status = Status.ONLINE;
		List<Datafile> datafiles = new ArrayList<Datafile>();
		List<Dataset> datasets = new ArrayList<Dataset>();
		try {
			// check, if ICAT will permit access to the requested files
			if (datafileIds != null) {
				List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
				for (String id : datafileIdList) {
					Datafile df = icatClient.getDatafileWithDatasetForDatafileId(sessionId,
							Long.parseLong(id));
					datafiles.add(df);
				}
			}
			if (datasetIds != null) {
				List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : datasetIdList) {
					Dataset ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId,
							Long.parseLong(id));
					datasets.add(ds);
				}
			}

			// check the files availability on fast storage
			for (Datafile df : datafiles) {
				if (!fastStorage.datafileExists(df.getLocation())) {
					status = Status.ARCHIVED;
					break;
				}
			}
			if (status != Status.ARCHIVED) {
				for (Dataset ds : datasets) {
					if (!fastStorage.datasetExists(ds.getLocation())) {
						status = Status.ARCHIVED;
						break;
					}
				}
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			switch (type) {
			case INSUFFICIENT_PRIVILEGES:
				throw new ForbiddenException(Code.DENIED,
						"You don't have sufficient privileges to perform this operation");
			case NO_SUCH_OBJECT_FOUND:
				throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
			default:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
						"Unrecognized ICAT exception " + e);
			}
		} catch (FileNotFoundException e) {
			throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
		} catch (IOException e) {
			throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
		}

		return Response.status(200).entity(status + "\n").build();
	}

	@GET
	@Path("getData")
	@Produces("application/octet-stream")
	public Response getData(@QueryParam("preparedId") String preparedId,
			@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds, @QueryParam("compress") String compress,
			@QueryParam("zip") String zip, @QueryParam("outname") String outname,
			@QueryParam("offset") String offsetString) throws BadRequestException,
			NotFoundException, InternalServerErrorException, ForbiddenException,
			NotImplementedException {
		Response data = null;
		long offset = 0;

		if (offsetString != null) {
			try {
				offset = Long.parseLong(offsetString);
				if (offset < 0) {
					throw new BadRequestException(Code.NEGATIVE,
							"The offset, if specified, must be a non-negative integer");
				}
			} catch (NumberFormatException e) {
				throw new BadRequestException(Code.NEGATIVE,
						"The offset, if specified, must be a non-negative integer");
			}
		}
		if (preparedId != null) {
			data = getData(preparedId, outname, offset);
		} else {
			data = getData(sessionId, investigationIds, datasetIds, datafileIds, compress, zip,
					outname, offset);
		}
		return data;
	}

	/**
	 * Returns a zip file containing the requested files.
	 * 
	 * TODO: find out how to catch the IOException in order to throw 404-file not longer in cache
	 * TODO: work out how to differentiate between NOT FOUND because of bad preparedId, INCOMPLETE,
	 * or some of the requested ids were not found
	 * 
	 * @param preparedId
	 *            The ID of the download request
	 * @param outname
	 *            The desired filename for the download (optional)
	 * @param offset
	 *            The desired offset of the file (optional)
	 * @return HTTP response containing the ZIP file
	 * @throws BadRequestException
	 * @throws NotFoundException
	 * @throws InternalServerErrorException
	 * @throws ForbiddenException
	 */
	private Response getData(String preparedId, String outname, final long offset)
			throws BadRequestException, NotFoundException, InternalServerErrorException,
			ForbiddenException {

		final IdsRequestEntity requestEntity;
		String name = null;

		if (ValidationHelper.isValidId(preparedId) == false) {
			throw new BadRequestException(Code.BAD_PREPARED_ID,
					"The preparedId parameter is invalid");
		}
		if (ValidationHelper.isValidName(outname) == false) {
			throw new BadRequestException(Code.BAD_OUTNAME, "The outname parameter is invalid");
		}

		logger.info("New webservice request: getData " + "preparedId='" + preparedId + "' "
				+ "outname='" + outname + "' " + "offset='" + offset + "'");

		List<IdsRequestEntity> requests = requestHelper.getRequestByPreparedId(preparedId);
		if (requests.size() == 0) {
			throw new NotFoundException(Code.NO_PREPARED_ID, "No matches found for preparedId \""
					+ preparedId + "\"");
		}
		if (requests.size() > 1) {
			String msg = "More than one match found for preparedId \"" + preparedId + "\"";
			logger.error(msg);
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, msg);
		}
		requestEntity = requests.get(0);

		// the internal download request status must be COMPLETE in order to
		// download the zip
		switch (requestEntity.getStatus()) {
		case SUBMITTED:
		case INFO_RETRIEVING:
		case INFO_RETRIEVED:
		case RETRIEVING:
			throw new NotFoundException(Code.CHECK_THIS_CODE,
					"Requested files are not ready for download");
		case DENIED:
			// TODO: return a list of the 'bad' files?
			throw new ForbiddenException(Code.DENIED,
					"You do not have permission to download one or more of the requested files");
		case NOT_FOUND:
			// TODO: return list of the 'bad' ids?
			throw new NotFoundException(Code.CHECK_THIS_CODE,
					"Some of the requested datafile / dataset ids were not found");
		case INCOMPLETE:
			throw new NotFoundException(Code.CHECK_THIS_CODE,
					"Some of the requested files are no longer available in ICAT.");
		case ERROR:
			// TODO: return list of the missing files?
			String msg = "Unable to find files in storage";
			logger.error(msg);
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, msg);
		case COMPLETED:
		default:
			break;
		}

		/* if no outname supplied give default name also suffix with .zip if absent */
		if (outname == null) {
			name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(requestEntity
					.getSubmittedTime());
			name = name + ".zip";
		} else {
			name = outname;
			String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
			if ("zip".equals(ext) == false) {
				name = name + ".zip";
			}
		}

		// create output stream of the zip file
		InputStream zipStream = null;
		try {
			zipStream = fastStorage.getPreparedZip(requestEntity.getPreparedId() + ".zip", offset);
			long skipped = zipStream.skip(offset);
			if (skipped != offset) {
				throw new IllegalArgumentException("Offset (" + offset
						+ " bytes) is larger than file size (" + skipped + " bytes)");
			}
			zipStream.skip(offset);
		} catch (IOException e) {
			throw new WebApplicationException(e);
		}

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				try {
					copy(fastStorage.getPreparedZip(requestEntity.getPreparedId() + ".zip", offset),
							output);
				} catch (Exception e) {
					throw new WebApplicationException(e);
				}
			}
		};
		return Response.ok(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();

	}

	private Response getData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, String compress, String zip, String outname, final long offset)
			throws BadRequestException, NotImplementedException, InternalServerErrorException,
			ForbiddenException, NotFoundException {

		String name = null;

		// 501
		if (investigationIds != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"investigationIds are not supported");
		}
		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(Code.BAD_SESSION_ID, "The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException(Code.BAD_DATASET_IDS,
					"The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException(Code.BAD_DATAFILE_IDS,
					"The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"At least one of datasetIds or datafileIds parameters must be set");
		}
		if (ValidationHelper.isValidName(outname) == false) {
			throw new BadRequestException(Code.BAD_OUTNAME, "The outname parameter is invalid");
		}

		logger.info(String
				.format("New webservice request: getData investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		final List<Datafile> datafiles = new ArrayList<>();
		final List<Dataset> datasets = new ArrayList<>();
		Status status = Status.ONLINE;
		IdsRequestEntity restoreRequest = null;
		try {
			// check, if ICAT will permit access to the requested files
			if (datafileIds != null) {
				List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
				for (String id : datafileIdList) {
					Datafile df = icatClient.getDatafileWithDatasetForDatafileId(sessionId,
							Long.parseLong(id));
					datafiles.add(df);
				}
			}
			if (datasetIds != null) {
				List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : datasetIdList) {
					Dataset ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId,
							Long.parseLong(id));
					datasets.add(ds);
				}
			}

			// check the files availability on fast storage
			for (Datafile df : datafiles) {
				if (!fastStorage.datafileExists(df.getLocation())) {
					status = Status.ARCHIVED;
					if (restoreRequest == null) {
						restoreRequest = requestHelper.createRestoreRequest(sessionId);
					}
					requestHelper.addDatafile(sessionId, restoreRequest, df);
				}
			}
			for (Dataset ds : datasets) {
				if (!fastStorage.datasetExists(ds.getLocation())) {
					status = Status.ARCHIVED;
					if (restoreRequest == null) {
						restoreRequest = requestHelper.createRestoreRequest(sessionId);
					}
					requestHelper.addDataset(sessionId, restoreRequest, ds);
				}
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			switch (type) {
			case INSUFFICIENT_PRIVILEGES:
				throw new ForbiddenException(Code.DENIED,
						"You don't have sufficient privileges to perform this operation");
			case NO_SUCH_OBJECT_FOUND:
				throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
			default:
				String msg = "Unrecognized ICAT exception " + e;
				logger.error(msg);
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE, msg);
			}
		} catch (FileNotFoundException e) {
			throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
		} catch (Throwable t) {
			String msg = t.getClass() + " " + t.getMessage();
			logger.error(msg);
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, msg);
		}

		if (restoreRequest != null) {
			for (IdsDataEntity de : restoreRequest.getDataEntities()) {
				queue(de, DeferredOp.RESTORE);
			}
		}
		if (status == Status.ARCHIVED) {
			return Response.status(202).build();
		}

		// if no outname supplied give default name also suffix with .zip if
		// absent
		if (outname == null) {
			name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
			name = name + ".zip";
		} else {
			name = outname;
			String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
			if ("zip".equals(ext) == false) {
				name = name + ".zip";
			}
		}

		final boolean finalCompress = "true".equals(compress) ? true : false;
		final String finalName = name;
		final String finalSessionId = sessionId;

		// create output stream of the zip file
		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				try {
					InputStream in = ZipHelper.prepareTemporaryZip(
							String.format("%s_%s", finalName, finalSessionId), datasets, datafiles,
							finalCompress, fastStorage);
					long skipped = in.skip(offset);
					if (skipped != offset) {
						throw new IllegalArgumentException("Offset (" + offset
								+ " bytes) is larger than file size (" + skipped + " bytes)");
					}
					copy(in, output);
				} catch (Exception e) {
					throw new WebApplicationException(e);
				}
			}
		};
		return Response.ok(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();
	}

	public static void copy(InputStream is, OutputStream os) throws IOException {
		BufferedInputStream bis = null;
		BufferedOutputStream bos = null;
		try {
			int bytesRead = 0;
			byte[] buffer = new byte[BUFSIZ];
			bis = new BufferedInputStream(is);
			bos = new BufferedOutputStream(os);

			// write bytes to output stream
			while ((bytesRead = bis.read(buffer)) > 0) {
				bos.write(buffer, 0, bytesRead);
			}
		} finally {
			if (bis != null) {
				bis.close();
			}
			if (bos != null) {
				bos.close();
			}
		}
	}

	@PUT
	@Path("put")
	@Consumes("application/octet-stream")
	public Response put(InputStream body, @QueryParam("sessionId") String sessionId,
			@QueryParam("name") String name, @QueryParam("datafileFormatId") Long datafileFormatId,
			@QueryParam("datasetId") String datasetId,
			@QueryParam("description") String description, @QueryParam("doi") String doi,
			@QueryParam("datafileCreateTime") String datafileCreateTime,
			@QueryParam("datafileModTime") String datafileModTime) throws Exception {
		logger.info(String.format("put received, name=%s", name));

		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(Code.BAD_SESSION_ID, "The sessionId parameter is invalid");
		}
		if (name == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING, "The name parameter must be set");
		}
		if (datafileFormatId == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"The datafileFormatId parameter must be set");
		}

		if (datasetId == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"The datasetId parameter must be set");
		}

		Dataset ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId,
				Long.parseLong(datasetId));

		if (!fastStorage.datasetExists(ds.getLocation())) {
			IdsRequestEntity requestEntity = requestHelper.createRestoreRequest(sessionId);
			requestHelper.addDatasets(sessionId, requestEntity, datasetId);
			this.queue(requestEntity.getDataEntities().get(0), DeferredOp.RESTORE);
			throw new NotFoundException(Code.CHECK_THIS_CODE,
					"Before putting a datafile, its dataset has to be restored, restoration requested automatically");
		}
		Datafile dummy = new Datafile();
		dummy.setName(name);
		dummy.setDataset(ds);
		long tbytes = fastStorage.putDatafile((new File(ds.getLocation(), name)).getPath(), body);
		registerDatafile(sessionId, name, datafileFormatId, tbytes, ds);
		// refresh the DS (contains the new DF)
		ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId, Long.parseLong(datasetId));
		// try {
		// InputStream is = ZipHelper.zipDataset(ds, false, fastStorage);
		// fastStorage.putDataset(ds, is);
		// } catch (IOException e) {
		// logger.error("Couldn't zip dataset " + ds + ", reason: " + e.getMessage());
		// throw new InternalServerErrorException(e.getMessage());
		// }

		IdsRequestEntity requestEntity = requestHelper.createWriteRequest(sessionId);
		requestHelper.addDataset(sessionId, requestEntity, ds);
		for (IdsDataEntity de : requestEntity.getDataEntities()) {
			queue(de, DeferredOp.WRITE);
		}

		return Response.status(201).entity(name).build();
	}

	@DELETE
	@Path("delete")
	@Produces("text/plain")
	public Response delete(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds) throws NotImplementedException,
			BadRequestException, ForbiddenException, NotFoundException,
			InternalServerErrorException {
		logger.info("delete received");
		// 501
		if (investigationIds != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"investigationIds are not supported");
		}
		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(Code.BAD_SESSION_ID, "The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException(Code.BAD_DATASET_IDS,
					"The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException(Code.BAD_DATAFILE_IDS,
					"The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"At least one of datasetIds or datafileIds parameters must be set");
		}

		// at this point we're sure, that all arguments are valid
		logger.info("New webservice request: delete " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		final List<Datafile> datafiles = new ArrayList<Datafile>();
		final List<Dataset> datasets = new ArrayList<Dataset>();
		Status status = Status.ONLINE;
		IdsRequestEntity restoreRequest = null;
		try {
			// check, if ICAT will permit access to the requested files
			if (datafileIds != null) {
				List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
				for (String id : datafileIdList) {
					Datafile df = icatClient.getDatafileWithDatasetForDatafileId(sessionId,
							Long.parseLong(id));
					datafiles.add(df);
				}
			}
			if (datasetIds != null) {
				List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : datasetIdList) {
					Dataset ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId,
							Long.parseLong(id));
					datasets.add(ds);
				}
			}

			// check the files availability on fast storage
			for (Datafile df : datafiles) {
				if (!fastStorage.datafileExists(df.getLocation())) {
					status = Status.ARCHIVED;
					if (restoreRequest == null) {
						restoreRequest = requestHelper.createRestoreRequest(sessionId);
					}
					requestHelper.addDatafile(sessionId, restoreRequest, df);
				}
			}
			for (Dataset ds : datasets) {
				if (!fastStorage.datasetExists(ds.getLocation())) {
					status = Status.ARCHIVED;
					if (restoreRequest == null) {
						restoreRequest = requestHelper.createRestoreRequest(sessionId);
					}
					requestHelper.addDataset(sessionId, restoreRequest, ds);
				}
			}

			if (restoreRequest != null) {
				for (IdsDataEntity de : restoreRequest.getDataEntities()) {
					queue(de, DeferredOp.RESTORE);
				}
			}
			if (status == Status.ARCHIVED) {
				throw new FileNotFoundException(
						"Some files have not been restored. Restoration requested");
			}

			IdsRequestEntity writeRequest = requestHelper.createWriteRequest(sessionId);
			for (Datafile df : datafiles) {
				icatClient.deleteDatafile(sessionId, df);
				// update dataset
				Dataset ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId, df
						.getDataset().getId());
				requestHelper.addDataset(sessionId, writeRequest, ds);
			}
			for (Dataset ds : datasets) {
				icatClient.deleteDataset(sessionId, ds);
				fastStorage.deleteDataset(ds.getLocation());
				// Dataset tmpDs = new Dataset();
				// tmpDs.setLocation(ds.getLocation());
				requestHelper.addDataset(sessionId, writeRequest, ds);
			}
			for (IdsDataEntity de : writeRequest.getDataEntities()) {
				queue(de, DeferredOp.WRITE);
			}

		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			switch (type) {
			case INSUFFICIENT_PRIVILEGES:
				throw new ForbiddenException(Code.DENIED,
						"You don't have sufficient privileges to perform this operation");
			case NO_SUCH_OBJECT_FOUND:
				throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
			default:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
						"Unrecognized ICAT exception " + e);
			}
		} catch (FileNotFoundException e) {
			throw new NotFoundException(Code.CHECK_THIS_CODE, e.getMessage());
		} catch (Exception e) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(Code.CHECK_THIS_CODE, t.getMessage());
		}

		return Response.status(200).build();
	}

	@POST
	@Path("restore")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response restore(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws NotImplementedException, BadRequestException, ForbiddenException,
			InternalServerErrorException {
		IdsRequestEntity requestEntity = null;
		logger.info("restore received");

		if (investigationIds != null) {
			throw new NotImplementedException(Code.NOT_IMPLEMENTED,
					"investigationIds are not supported");
		}

		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException(Code.BAD_SESSION_ID, "The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException(Code.BAD_DATASET_IDS,
					"The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException(Code.BAD_DATAFILE_IDS,
					"The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(Code.PARAMETER_MISSING,
					"At least one of datasetIds or datafileIds parameters must be set");
		}
		// at this point we're sure, that all arguments are valid
		logger.info("New webservice request: restore " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		try {
			requestEntity = requestHelper.createRestoreRequest(sessionId);
			if (datafileIds != null) {
				requestHelper.addDatafiles(sessionId, requestEntity, datafileIds);
			}
			if (datasetIds != null) {
				requestHelper.addDatasets(sessionId, requestEntity, datasetIds);
			}
			for (IdsDataEntity de : requestEntity.getDataEntities()) {
				this.queue(de, DeferredOp.RESTORE);
			}
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			switch (type) {
			case SESSION:
				throw new ForbiddenException(Code.DENIED,
						"The sessionId parameter is invalid or has expired");
			case INTERNAL:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE, e.getMessage());
			default:
				throw new InternalServerErrorException(Code.CHECK_THIS_CODE,
						"Unexpected ICAT exception " + type + " " + e.getMessage());
			}
			// } catch (PersistenceException e) {
			// throw new InternalServerErrorException("Unable to connect to the database");
			// } catch (Exception e) {
			// throw new InternalServerErrorException(e.getMessage());
			// } catch (Throwable t) {
			// throw new InternalServerErrorException(t.getMessage());
		}
		Response response = Response.ok().build();
		logger.debug("Response: " + response.getStatus());
		return response;
	}

	private void queue(IdsDataEntity de, DeferredOp deferredOp) {
		logger.info("Requesting " + deferredOp + " of " + de);

		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();

		synchronized (deferredOpsQueue) {
			// PREPARE is a special case, as it's independent of the FSM state
			if (deferredOp == DeferredOp.PREPARE) {
				deferredOpsQueue.put(de, RequestedState.PREPARE_REQUESTED);
				return;
			}

			RequestedState state = null;
			// If we are overwriting a DE from a different request, we should
			// set its status to INCOMPLETE and remove it from the
			// deferredOpsQueue. So far the previous RequestedState will be set
			// at most once, so there's no ambiguity. Be careful though when
			// implementing Investigations!
			Iterator<IdsDataEntity> iter = deferredOpsQueue.keySet().iterator();
			while (iter.hasNext()) {
				IdsDataEntity oldDe = iter.next();
				if (oldDe.overlapsWith(de)) {
					logger.info(String.format("%s will replace %s in the queue", de, oldDe));
					requestHelper.setDataEntityStatus(oldDe, StatusInfo.INCOMPLETE);
					state = deferredOpsQueue.get(oldDe);
					iter.remove();
					break;
				}
			}
			// if there's no overlapping DE, or the one overlapping is to be
			// prepared we can safely create a new request (DEs scheduled for
			// preparation are processed independently)
			if (state == null || state == RequestedState.PREPARE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_REQUESTED);
					this.setDelay(de.getIcatDataset());
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(de, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(de, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_REQUESTED);
					this.setDelay(de.getIcatDataset());
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(de, RequestedState.RESTORE_REQUESTED);
				} else {
					deferredOpsQueue.put(de, RequestedState.ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.RESTORE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_REQUESTED);
					this.setDelay(de.getIcatDataset());
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(de, RequestedState.ARCHIVE_REQUESTED);
				} else {
					deferredOpsQueue.put(de, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_REQUESTED);
					this.setDelay(de.getIcatDataset());
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				} else {
					deferredOpsQueue.put(de, RequestedState.WRITE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
					this.setDelay(de.getIcatDataset());
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(de, RequestedState.WRITE_REQUESTED);
				} else {
					deferredOpsQueue.put(de, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				}
			}
		}
	}

	private void setDelay(Dataset ds) {
		long newWriteTime = System.currentTimeMillis() + archiveWriteDelayMillis;
		Map<Dataset, Long> writeTimes = requestQueues.getWriteTimes();
		writeTimes.put(ds, newWriteTime);
		logger.info("Requesting delay of writing of " + ds.getId() + " till " + newWriteTime);
	}

	private Long registerDatafile(String sessionid, String name, long datafileFormatId,
			long tbytes, Dataset dataset) throws Exception {
		final Datafile df = new Datafile();
		String location = new File(dataset.getLocation(), name).getPath();
		DatafileFormat format = icatClient.findDatafileFormatById(sessionid, datafileFormatId);
		if (format == null) {
			throw new NotFoundException(Code.CHECK_THIS_CODE, "DatafileFormatId "
					+ datafileFormatId + " does not exist.");
		}
		df.setDatafileFormat(format);
		df.setLocation(location);
		df.setFileSize(tbytes);
		df.setName(name);
		df.setDataset(dataset);
		df.setId((Long) icatClient.registerDatafile(sessionid, df));
		logger.debug("Registered datafile for dataset {} for {}", dataset.getId(), name + " at "
				+ location);
		return df.getId();
	}

	private void restartUnfinishedWork() {
		List<IdsRequestEntity> requests = requestHelper.getUnfinishedRequests();
		for (IdsRequestEntity request : requests) {
			for (IdsDataEntity de : request.getDataEntities()) {
				queue(de, request.getDeferredOp());
			}
		}
	}
}