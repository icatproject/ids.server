package org.icatproject.ids.webservice;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.entity.IdsRequestEntity;
import org.icatproject.ids.plugin.StorageInterface;
import org.icatproject.ids.thread.ProcessQueue;
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

@Stateless
public class IdsBean {

	private static final int BUFSIZ = 2048;

	private final static Logger logger = LoggerFactory.getLogger(IdsBean.class);

	private static void copy(InputStream is, OutputStream os) throws IOException {
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

	private long archiveWriteDelayMillis = PropertyHandler.getInstance().getWriteDelaySeconds() * 1000L;

	private DatatypeFactory datatypeFactory;

	private StorageInterface fastStorage;

	@EJB
	private RequestHelper requestHelper;

	private RequestQueues requestQueues = RequestQueues.getInstance();

	private Timer timer = new Timer();

	private ICAT icat;

	public Response archive(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, InternalException, NotFoundException {

		logger.info("New webservice request: archive " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

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
		} catch (RuntimeException e) {
			processRuntimeException(e);
		}

		return Response.ok().build();

	}

	public Response delete(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, InternalException, NotFoundException {

		logger.info("New webservice request: delete " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

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

		final List<Datafile> datafiles = new ArrayList<Datafile>();
		final List<Dataset> datasets = new ArrayList<Dataset>();
		Status status = Status.ONLINE;
		IdsRequestEntity restoreRequest = null;
		try {
			// check, if ICAT will permit access to the requested files
			if (datafileIds != null) {
				List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
				for (String id : datafileIdList) {
					Datafile df = (Datafile) icat.get(sessionId, "Datafile INCLUDE Dataset",
							Long.parseLong(id));
					datafiles.add(df);
				}
			}
			if (datasetIds != null) {
				List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : datasetIdList) {
					Dataset ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile",
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
				icat.delete(sessionId, df);
				// update dataset
				Dataset ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", df
						.getDataset().getId());
				requestHelper.addDataset(sessionId, writeRequest, ds);
			}
			for (Dataset ds : datasets) {
				icat.delete(sessionId, ds);
				fastStorage.deleteDataset(ds.getLocation());
				requestHelper.addDataset(sessionId, writeRequest, ds);
			}
			for (IdsDataEntity de : writeRequest.getDataEntities()) {
				queue(de, DeferredOp.WRITE);
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
			throw new NotFoundException(e.getMessage());
		} catch (RuntimeException e) {
			processRuntimeException(e);
		}

		return Response.status(200).build();
	}

	public Response getData(String preparedId, String outname, final long offset)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException {

		final IdsRequestEntity requestEntity;
		String name = null;

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
		requestEntity = requests.get(0);

		StatusInfo statusInfo = requestEntity.getStatus();
		Status status = statusInfo.getUserStatus();
		if (status == null) {
			if (statusInfo == StatusInfo.DENIED) {
				throw new InsufficientPrivilegesException(
						"You do not have permission to download one or more of the requested files");
			} else if (statusInfo == StatusInfo.NOT_FOUND) {
				throw new NotFoundException("Some of the requested data ids were not found");
			} else if (statusInfo == StatusInfo.ERROR) {
				String msg = "Unable to find files in storage";
				logger.error(msg);
				throw new InternalException(msg);
			}
		} else if (status == Status.INCOMPLETE) {
			throw new NotFoundException(
					"Some of the requested files are no longer available in ICAT.");
		} else if (status == Status.RESTORING) {
			throw new NotFoundException("Requested files are not ready for download");
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
					logger.debug("Requested offset " + offset);
					long skipped = 0;
					try {
						skipped = input.skip(offset);
						logger.debug("Skipped " + skipped);
					} catch (IOException e) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity(e.getClass() + " " + e.getMessage()).build());
					}
					if (skipped != offset) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity("Offset (" + offset + " bytes) is larger than file size ("
										+ skipped + " bytes)").build());
					}
					copy(input, output);
				} catch (IOException e) {
					throw new WebApplicationException(Response
							.status(HttpURLConnection.HTTP_INTERNAL_ERROR)
							.entity(e.getClass() + " " + e.getMessage()).build());
				}
			}
		};
		return Response
				.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();

	}

	public Response getData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, Boolean compress, Boolean zip, String outname, final long offset)
			throws BadRequestException, NotImplementedException, InternalException,
			InsufficientPrivilegesException, NotFoundException, DataNotOnlineException {
		
		logger.info(String
				.format("New webservice request: getData investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

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

		//TODO add investigations
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
					Datafile df = (Datafile) icat.get(sessionId, "Datafile INCLUDE Dataset",
							Long.parseLong(id));
					datafiles.add(df);
				}
			}
			if (datasetIds != null) {
				List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : datasetIdList) {
					Dataset ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile",
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
		} catch (RuntimeException e) {
			processRuntimeException(e);
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
		String name;
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
						skipped = input.skip(offset);
					} catch (IOException e) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity(e.getClass() + " " + e.getMessage()).build());
					}
					if (skipped != offset) {
						throw new WebApplicationException(Response
								.status(HttpURLConnection.HTTP_BAD_REQUEST)
								.entity("Offset (" + offset
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
		return Response
				.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();
	}

	public Response getStatus(String preparedId) throws BadRequestException, NotFoundException,
			InternalException, InsufficientPrivilegesException, NotImplementedException {

		logger.info("New webservice request: getStatus " + "preparedId='" + preparedId + "'");

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
		StatusInfo statusInfo = requests.get(0).getStatus();

		Status status = statusInfo.getUserStatus();
		if (status == null) {
			if (statusInfo == StatusInfo.DENIED) {
				throw new InsufficientPrivilegesException(
						"You do not have permission to download one or more of the requested files");
			} else if (statusInfo == StatusInfo.NOT_FOUND) {
				throw new NotFoundException(
						"Some of the requested datafile / dataset ids were not found");
			} else if (statusInfo == StatusInfo.ERROR) {
				throw new InternalException("Unable to find files in storage");
			}
		}
		return Response.ok(status.name()).build();

	}

	public Response getStatus(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException {

		logger.info(String
				.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
						investigationIds, datasetIds, datafileIds));

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

		Status status = Status.ONLINE;
		List<Datafile> datafiles = new ArrayList<Datafile>();
		List<Dataset> datasets = new ArrayList<Dataset>();
		try {
			// check, if ICAT will permit access to the requested files
			if (datafileIds != null) {
				List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
				for (String id : datafileIdList) {
					Datafile df = (Datafile) icat.get(sessionId, "Datafile INCLUDE Dataset",
							Long.parseLong(id));
					datafiles.add(df);
				}
			}
			if (datasetIds != null) {
				List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : datasetIdList) {
					Dataset ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile",
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
		} catch (RuntimeException e) {
			processRuntimeException(e);
		}

		logger.debug("Status is " + status.name());
		return Response.ok(status.name()).build();

	}

	@PostConstruct
	private void init() throws DatatypeConfigurationException {
		logger.info("creating IdsBean");
		PropertyHandler ph = PropertyHandler.getInstance();
		fastStorage = ph.getMainStorage();
		datatypeFactory = DatatypeFactory.newInstance();

		timer.schedule(new ProcessQueue(timer, requestHelper),
				ph.getProcessQueueIntervalSeconds() * 1000L);
		icat = PropertyHandler.getInstance().getIcatService();

		restartUnfinishedWork();

		logger.info("created IdsBean");
	}

	public Response prepareData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, boolean compress, boolean zip) throws NotImplementedException,
			BadRequestException, InternalException, InsufficientPrivilegesException,
			NotFoundException {

		logger.info("New webservice request: prepareData " + "investigationIds='"
				+ investigationIds + "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='"
				+ datafileIds + "' " + "compress='" + compress + "' " + "zip='" + zip + "'");

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

		IdsRequestEntity requestEntity = null;
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
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());

		} catch (RuntimeException e) {
			processRuntimeException(e);
		}
		return Response.status(200).entity(requestEntity.getPreparedId()).build();
	}

	private void processRuntimeException(RuntimeException e) throws InternalException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		e.printStackTrace(new PrintStream(baos));
		logger.debug(baos.toString());
		throw new InternalException(e.getClass() + " " + e.getMessage());
	}

	public Response put(InputStream body, String sessionId, String name, long datafileFormatId,
			long datasetId, String description, String doi, Long datafileCreateTime,
			Long datafileModTime) throws NotFoundException, DataNotOnlineException,
			BadRequestException, InsufficientPrivilegesException, InternalException {
		ValidationHelper.validateUUID("sessionId", sessionId);

		if (name == null) {
			throw new BadRequestException("The name parameter must be set");
		}

		logger.info("New webservice request: put " + "name='" + name + "' " + "datafileFormatId='"
				+ datafileFormatId + "' " + "datasetId='" + datasetId + "' " + "description='"
				+ description + "' " + "doi='" + doi + "' " + "datafileCreateTime='"
				+ datafileCreateTime + "' " + "datafileModTime='" + datafileModTime + "'");

		Dataset ds;
		try {
			ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", datasetId);
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
				ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", datasetId);
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
		} catch (RuntimeException e) {
			processRuntimeException(e);
			return null; // Will never get here but the compiler doesn't know
		}
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

	private Long registerDatafile(String sessionId, String name, long datafileFormatId,
			long tbytes, Dataset dataset, String description, String doi, Long datafileCreateTime,
			Long datafileModTime) throws InsufficientPrivilegesException, NotFoundException,
			InternalException, BadRequestException {
		final Datafile df = new Datafile();
		String location = new File(dataset.getLocation(), name).getPath();
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

	private void restartUnfinishedWork() {
		List<IdsRequestEntity> requests = requestHelper.getUnfinishedRequests();
		for (IdsRequestEntity request : requests) {
			for (IdsDataEntity de : request.getDataEntities()) {
				queue(de, request.getDeferredOp());
			}
		}
	}

	public Response restore(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws BadRequestException, NotImplementedException,
			InsufficientPrivilegesException, InternalException, NotFoundException {

		logger.info("New webservice request: restore " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

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
		;

		IdsRequestEntity requestEntity = null;
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
			if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
					|| type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
				throw new NotFoundException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		} catch (RuntimeException e) {
			processRuntimeException(e);
		}
		return Response.ok().build();
	}

	private void setDelay(Dataset ds) {
		long newWriteTime = System.currentTimeMillis() + archiveWriteDelayMillis;
		Map<Dataset, Long> writeTimes = requestQueues.getWriteTimes();
		writeTimes.put(ds, newWriteTime);
		logger.info("Requesting delay of writing of " + ds.getId() + " till " + newWriteTime);
	}

}
