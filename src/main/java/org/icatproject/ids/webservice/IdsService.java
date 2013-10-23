package org.icatproject.ids.webservice;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
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
import javax.xml.datatype.DatatypeFactory;

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
import org.icatproject.ids.webservice.exceptions.DataNotOnlineException;
import org.icatproject.ids.webservice.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.webservice.exceptions.InternalException;
import org.icatproject.ids.webservice.exceptions.NotFoundException;
import org.icatproject.ids.webservice.exceptions.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@Stateless
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

	private DatatypeFactory datatypeFactory;

	@PostConstruct
	public void postConstructInit() throws Exception {
		logger.info("creating WebService");
		PropertyHandler ph = PropertyHandler.getInstance();
		fastStorage = ph.getMainStorage();
		datatypeFactory = DatatypeFactory.newInstance();

		timer.schedule(new ProcessQueue(timer, requestHelper),
				ph.getProcessQueueIntervalSeconds() * 1000L);
		restartUnfinishedWork();
		logger.info("created WebService");
	}

	@POST
	@Path("prepareData")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response prepareData(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds,
			@FormParam("datafileIds") String datafileIds, @FormParam("compress") boolean compress,
			@FormParam("zip") boolean zip) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException {
		IdsRequestEntity requestEntity = null;
		
		logger.info("New webservice request: prepareData " + "investigationIds='"
				+ investigationIds + "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='"
				+ datafileIds + "' " + "compress='" + compress + "' " + "zip='" + zip + "'");

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}
		if (zip) {
			throw new NotImplementedException("the zip option is not supported");
		}
		if (compress) {
			throw new NotImplementedException("the compress option is not supported");
		}

		ValidationHelper.validateUUID("sessionId", sessionId);
		ValidationHelper.validateIdList("datasetIds", datasetIds);
		ValidationHelper.validateIdList("datafileIds", datafileIds);

		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(
					"At least one of datasetIds or datafileIds parameters must be set");
		}

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
				throw new InsufficientPrivilegesException(
						"The sessionId parameter is invalid or has expired");
			case INSUFFICIENT_PRIVILEGES:
				requestHelper.setRequestStatus(requestEntity, StatusInfo.ERROR);
				throw new InsufficientPrivilegesException(
						"You don't have sufficient privileges to perform this operation");
			case NO_SUCH_OBJECT_FOUND:
				requestHelper.setRequestStatus(requestEntity, StatusInfo.NOT_FOUND);
				throw new NotFoundException("Could not find requested objects");
			case INTERNAL:
				throw new InternalException("Unable to connect to ICAT server");
			default:
				throw new InternalException("Unrecognized ICAT exception " + e);
			}
		} catch (PersistenceException e) {
			throw new InternalException("Unable to connect to the database");
		} catch (Exception e) {
			throw new InternalException(e.getMessage());
		} catch (Throwable t) {
			throw new InternalException(t.getMessage());
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
	 * @throws InsufficientPrivilegesException
	 * @throws NotImplementedException
	 * @throws InternalException
	 */
	@POST
	@Path("archive")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response archive(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws BadRequestException, InsufficientPrivilegesException, NotImplementedException,
			InternalException, NotFoundException {
		IdsRequestEntity requestEntity = null;

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}
		ValidationHelper.validateUUID("sessionId", sessionId);
		ValidationHelper.validateIdList("datasetIds", datasetIds);
		ValidationHelper.validateIdList("datafileIds", datafileIds);
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(
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
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		}

		return Response.ok().build();
	}

	@GET
	@Path("getStatus")
	@Produces("text/plain")
	public Response getStatus(@QueryParam("preparedId") String preparedId,
			@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafilesIds) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
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
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 */
	private Response getStatus(String preparedId) throws BadRequestException, NotFoundException,
			InternalException, InsufficientPrivilegesException, NotImplementedException {

		ValidationHelper.validateUUID("preparedId", preparedId);

		List<IdsRequestEntity> requests = requestHelper.getRequestByPreparedId(preparedId);
		if (requests.size() == 0) {
			throw new NotFoundException("No matches found for preparedId \"" + preparedId + "\"");
		}
		if (requests.size() > 1) {
			String msg = "More than one match found for preparedId \"" + preparedId + "\"";
			logger.error(msg);
			throw new InternalException(msg);
		}
		String status = requests.get(0).getStatus().name();

		logger.info("New webservice request: getStatus " + "preparedId='" + preparedId + "'");

		// convert internal status to appropriate external status
		StatusInfo statusinfo = StatusInfo.valueOf(status);
		if (statusinfo == StatusInfo.COMPLETED) {
			status = Status.ONLINE.name();
		} else if (statusinfo == StatusInfo.INCOMPLETE) {
			status = Status.INCOMPLETE.name();
		} else if (statusinfo == StatusInfo.DENIED) {
			throw new InsufficientPrivilegesException(
					"You do not have permission to download one or more of the requested files");
		} else if (statusinfo == StatusInfo.NOT_FOUND) {
			throw new NotFoundException(
					"Some of the requested datafile / dataset ids were not found");
		} else if (statusinfo == StatusInfo.ERROR) {
			throw new InternalException("Unable to find files in storage");
		} else {
			status = Status.RESTORING.name();
		}

		return Response.status(200).entity(status).build();
	}

	private Response getStatus(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException {

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}

		ValidationHelper.validateUUID("sessionId", sessionId);
		ValidationHelper.validateIdList("datasetIds", datasetIds);
		ValidationHelper.validateIdList("datafileIds", datafileIds);
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(
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
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES) {
				throw new InsufficientPrivilegesException(e.getMessage());
			} else if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			} else {
				throw new InternalException("Unexpected ICAT exception " + type + " "
						+ e.getMessage());
			}
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

		logger.debug(status.name());
		return Response.status(200).entity(status.name()).build();
	}

	@GET
	@Path("getData")
	@Produces("application/octet-stream")
	public Response getData(@QueryParam("preparedId") String preparedId,
			@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds,
			@QueryParam("compress") boolean compress, @QueryParam("zip") boolean zip,
			@QueryParam("outname") String outname, @QueryParam("offset") long offset)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException {
		Response data = null;

		if (offset < 0) {
			throw new BadRequestException(
					"The offset, if specified, must be a non-negative integer");
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
	 * @throws InternalException
	 * @throws InsufficientPrivilegesException
	 */
	private Response getData(String preparedId, String outname, Long offset)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException {

		final IdsRequestEntity requestEntity;
		String name = null;

		ValidationHelper.validateUUID("preparedId", preparedId);

		logger.info("New webservice request: getData " + "preparedId='" + preparedId + "' "
				+ "outname='" + outname + "' " + "offset='" + offset + "'");

		List<IdsRequestEntity> requests = requestHelper.getRequestByPreparedId(preparedId);
		if (requests.size() == 0) {
			throw new NotFoundException("No matches found for preparedId \"" + preparedId + "\"");
		}
		if (requests.size() > 1) {
			String msg = "More than one match found for preparedId \"" + preparedId + "\"";
			logger.error(msg);
			throw new InternalException(msg);
		}
		requestEntity = requests.get(0);

		final long finalOffset = offset == null ? 0L : offset;

		// the internal download request status must be COMPLETE in order to
		// download the zip
		switch (requestEntity.getStatus()) {
		case SUBMITTED:
		case INFO_RETRIEVING:
		case INFO_RETRIEVED:
		case RETRIEVING:
			throw new NotFoundException("Requested files are not ready for download");
		case DENIED:
			// TODO: return a list of the 'bad' files?
			throw new InsufficientPrivilegesException(
					"You do not have permission to download one or more of the requested files");
		case NOT_FOUND:
			// TODO: return list of the 'bad' ids?
			throw new NotFoundException(
					"Some of the requested datafile / dataset ids were not found");
		case INCOMPLETE:
			throw new NotFoundException(
					"Some of the requested files are no longer available in ICAT.");
		case ERROR:
			// TODO: return list of the missing files?
			String msg = "Unable to find files in storage";
			logger.error(msg);
			throw new InternalException(msg);
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
		logger.debug("Outname set to " + name);

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {
				try {
					InputStream input = fastStorage.getPreparedZip(requestEntity.getPreparedId()
							+ ".zip");
					logger.debug("Requested offset " + finalOffset);
					long skipped = 0;
					try {
						skipped = input.skip(finalOffset);
						logger.debug("Skipped " + skipped);
					} catch (IOException e) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity(e.getClass() + " " + e.getMessage()).build());
					}
					if (skipped != finalOffset) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity("Offset (" + finalOffset
										+ " bytes) is larger than file size (" + skipped
										+ " bytes)").build());
					}
					copy(input, output);
				} catch (IOException e) {
					throw new WebApplicationException(Response
							.status(HttpURLConnection.HTTP_INTERNAL_ERROR)
							.entity(e.getClass() + " " + e.getMessage()).build());
				}
			}
		};
		return Response.ok(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();

	}

	private Response getData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, Boolean compress, Boolean zip, String outname, Long offset)
			throws BadRequestException, NotImplementedException, InternalException,
			InsufficientPrivilegesException, NotFoundException, DataNotOnlineException {

		String name = null;

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}
		ValidationHelper.validateUUID("sessionId", sessionId);
		ValidationHelper.validateIdList("datasetIds", datasetIds);
		ValidationHelper.validateIdList("datafileIds", datafileIds);
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(
					"At least one of datasetIds or datafileIds parameters must be set");
		}

		final long finalOffset = offset == null ? 0L : offset;

		logger.info(String
				.format("New webservice request: getData investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

		final List<Datafile> datafiles = new ArrayList<>();
		final List<Dataset> datasets = new ArrayList<>();
		Status status = Status.ONLINE;
		IdsRequestEntity restoreRequest = null;
		DataNotOnlineException exc = null;
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
					exc = new DataNotOnlineException("Current status of Datafile " + df.getId()
							+ " with location " + df.getLocation()
							+ " is ARCHIVED. It is now being restored.");
				}
			}
			for (Dataset ds : datasets) {
				if (!fastStorage.datasetExists(ds.getLocation())) {
					status = Status.ARCHIVED;
					if (restoreRequest == null) {
						restoreRequest = requestHelper.createRestoreRequest(sessionId);
					}
					requestHelper.addDataset(sessionId, restoreRequest, ds);
					exc = new DataNotOnlineException("Current status of Dataset " + ds.getId()
							+ " with location " + ds.getLocation()
							+ " is ARCHIVED. It is now being restored.");
				}
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
		} catch (IOException e) {
			throw new InternalException("IOException " + e.getMessage());
		}

		if (restoreRequest != null) {
			for (IdsDataEntity de : restoreRequest.getDataEntities()) {
				queue(de, DeferredOp.RESTORE);
			}
		}
		if (status == Status.ARCHIVED) {
			throw exc;
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

		logger.debug("About to return data");

		// create output stream of the zip file
		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {

				try {
					InputStream input = ZipHelper.prepareTemporaryZip(
							String.format("%s_%s", finalName, finalSessionId), datasets, datafiles,
							finalCompress, fastStorage);

					long skipped = 0;
					try {
						skipped = input.skip(finalOffset);
					} catch (IOException e) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity(e.getClass() + " " + e.getMessage()).build());
					}
					if (skipped != finalOffset) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity("Offset (" + finalOffset
										+ " bytes) is larger than file size (" + skipped
										+ " bytes)").build());
					}
					copy(input, output);
				} catch (IOException e) {
					throw new WebApplicationException(Response
							.status(HttpURLConnection.HTTP_INTERNAL_ERROR)
							.entity(e.getClass() + " " + e.getMessage()).build());
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

	@GET
	@Path("ping")
	public String ping() {
		logger.debug("ping request received");
		return "IdsOK";
	}

	@PUT
	@Path("put")
	@Consumes("application/octet-stream")
	public Response put(InputStream body, @QueryParam("sessionId") String sessionId,
			@QueryParam("name") String name, @QueryParam("datafileFormatId") Long datafileFormatId,
			@QueryParam("datasetId") long datasetId, @QueryParam("description") String description,
			@QueryParam("doi") String doi,
			@QueryParam("datafileCreateTime") Long datafileCreateTime,
			@QueryParam("datafileModTime") Long datafileModTime) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException {

		ValidationHelper.validateUUID("sessionId", sessionId);

		if (name == null) {
			throw new BadRequestException("The name parameter must be set");
		}
		if (datafileFormatId == null) {
			throw new BadRequestException("The datafileFormatId parameter must be set");
		}

		logger.info("New webservice request: put " + "name='" + name + "' " + "datafileFormatId='"
				+ datafileFormatId + "' " + "datasetId='" + datasetId + "' " + "description='"
				+ description + "' " + "doi='" + doi + "' " + "datafileCreateTime='"
				+ datafileCreateTime + "' " + "datafileModTime='" + datafileModTime + "'");

		Dataset ds;
		try {
			ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId, datasetId);
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
			if (!fastStorage.datasetExists(ds.getLocation())) {
				IdsRequestEntity requestEntity = requestHelper.createRestoreRequest(sessionId);
				requestHelper.addDataset(sessionId, requestEntity, datasetId);
				this.queue(requestEntity.getDataEntities().get(0), DeferredOp.RESTORE);
				throw new DataNotOnlineException(
						"Before putting a datafile, its dataset has to be restored, restoration requested automatically");
			}

			Datafile dummy = new Datafile();
			dummy.setName(name);
			dummy.setDataset(ds);
			long tbytes = fastStorage.putDatafile((new File(ds.getLocation(), name)).getPath(),
					body);
			Long dfId = registerDatafile(sessionId, name, datafileFormatId, tbytes, ds,
					description, doi, datafileCreateTime, datafileModTime);
			// refresh the DS (contains the new DF)
			try {
				ds = icatClient.getDatasetWithDatafilesForDatasetId(sessionId, datasetId);
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

			IdsRequestEntity requestEntity = requestHelper.createWriteRequest(sessionId);
			requestHelper.addDataset(sessionId, requestEntity, ds);
			for (IdsDataEntity de : requestEntity.getDataEntities()) {
				queue(de, DeferredOp.WRITE);
			}

			return Response.status(HttpURLConnection.HTTP_CREATED).entity(Long.toString(dfId))
					.build();

		} catch (IOException e) {
			throw new InternalException("IOEXception " + e.getMessage());
		}

	}

	@DELETE
	@Path("delete")
	@Produces("text/plain")
	public Response delete(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException {
		logger.info("delete received");

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}
		ValidationHelper.validateUUID("sessionId", sessionId);
		ValidationHelper.validateIdList("datasetIds", datasetIds);
		ValidationHelper.validateIdList("datafileIds", datafileIds);
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(
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
				throw new InsufficientPrivilegesException(
						"You don't have sufficient privileges to perform this operation");
			case NO_SUCH_OBJECT_FOUND:
				throw new NotFoundException(e.getMessage());
			default:
				throw new InternalException("Unrecognized ICAT exception " + e);
			}
		} catch (FileNotFoundException e) {
			throw new NotFoundException(e.getMessage());
		} catch (Exception e) {
			throw new InternalException(e.getMessage());
		} catch (Throwable t) {
			throw new InternalException(t.getMessage());
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
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {
		IdsRequestEntity requestEntity = null;
		logger.info("restore received");

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}

		ValidationHelper.validateUUID("sessionId", sessionId);
		ValidationHelper.validateIdList("datasetIds", datasetIds);
		ValidationHelper.validateIdList("datafileIds", datafileIds);
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException(
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
				throw new InsufficientPrivilegesException(
						"The sessionId parameter is invalid or has expired");
			case INTERNAL:
				throw new InternalException(e.getMessage());
			default:
				throw new InternalException("Unexpected ICAT exception " + type + " "
						+ e.getMessage());
			}

		}
		return Response.ok().build();
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
			long tbytes, Dataset dataset, String description, String doi, Long datafileCreateTime,
			Long datafileModTime) throws InsufficientPrivilegesException, NotFoundException,
			InternalException, BadRequestException {
		final Datafile df = new Datafile();
		String location = new File(dataset.getLocation(), name).getPath();
		DatafileFormat format;
		try {
			format = icatClient.findDatafileFormatById(sessionid, datafileFormatId);
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
		df.setFileSize(tbytes);
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
			df.setId((Long) icatClient.registerDatafile(sessionid, df));
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

	private void restartUnfinishedWork() {
		List<IdsRequestEntity> requests = requestHelper.getUnfinishedRequests();
		for (IdsRequestEntity request : requests) {
			for (IdsDataEntity de : request.getDataEntities()) {
				queue(de, request.getDeferredOp());
			}
		}
	}
}