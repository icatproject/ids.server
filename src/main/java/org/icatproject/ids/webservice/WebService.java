package org.icatproject.ids.webservice;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.EJBException;
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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.icatproject.ICAT;
import org.icatproject.ids.entity.DownloadRequestEntity;
import org.icatproject.ids.icatclient.ICATClientBase;
import org.icatproject.ids.icatclient.ICATClientFactory;
import org.icatproject.ids.icatclient.exceptions.ICATInternalException;
import org.icatproject.ids.icatclient.exceptions.ICATSessionException;
import org.icatproject.ids.queues.InfoRetrievalQueueSender;
import org.icatproject.ids.util.DownloadRequestHelper;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.util.ValidationHelper;
import org.icatproject.ids.webservice.exceptions.BadRequestException;
import org.icatproject.ids.webservice.exceptions.ForbiddenException;
import org.icatproject.ids.webservice.exceptions.InternalServerErrorException;
import org.icatproject.ids.webservice.exceptions.NotFoundException;
import org.icatproject.ids.webservice.exceptions.NotImplementedException;
import org.icatproject.ids2.ported.DeferredOp;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.icatproject.ids2.ported.entity.Ids2DatafileEntity;
import org.icatproject.ids2.ported.entity.Ids2DatasetEntity;
import org.icatproject.ids2.ported.entity.RequestEntity;
import org.icatproject.ids2.ported.thread.ProcessQueue;

/**
 * Implementation of the IDS specification for the IDS Reference Implementation.
 * Only the download related methods have been implemented.
 */
@Path("/")
@Stateless
public class WebService {

	private final static Logger logger = Logger.getLogger(WebService.class.getName());
	
	private ICAT icatClient;
	private long archiveWriteDelayMillis = PropertyHandler.getInstance().getWriteDelaySeconds() * 1000L;
	private Timer timer = new Timer();	
	private RequestQueues requestQueues = RequestQueues.getInstance();

	@EJB
	private InfoRetrievalQueueSender infoRetrievalQueueSender;

	@EJB
	private DownloadRequestHelper downloadRequestHelper;

	@EJB
	private RequestHelper requestHelper;

	@PostConstruct
	public void postConstructInit() {
		logger.info("creating WebService");
//		try {
//			final URL icatUrl = new URL(PropertyHandler.getInstance().getIcatURL());
//			final ICATService icatService = new ICATService(icatUrl, new QName(
//					"http://icatproject.org", "ICATService"));
//			this.icatClient = icatService.getICATPort();
//			this.timer = new Timer();
			timer.schedule(new ProcessQueue(timer, requestHelper), 
					PropertyHandler.getInstance().getProcessQueueIntervalSeconds() * 1000L);
//		} catch (Exception e) {
//			throw new InternalServerErrorException("Could not initialize ICAT client");
//		}
	}
	
	@PreDestroy
	public void destroy() {
		
	}

	/**
	 * Creates a new download request. Does not accept investigationIds or the
	 * zip parameter as all requested files will always be returned in a zip
	 * file.
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
	 *            Compress ZIP archive of files (dependent on ZIP parameter
	 *            below) (optional)
	 * @param zip
	 *            Request data to be packaged in a ZIP archive (not implemented)
	 * @return HTTP response containing a preparedId for the download request
	 */
	@POST
	@Path("prepareData")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response prepareData(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
			@FormParam("datafileIds") String datafileIds,
			@DefaultValue("false") @FormParam("compress") String compress, @FormParam("zip") String zip) {
//		DownloadRequestEntity downloadRequestEntity = null;
		RequestEntity requestEntity = null;
		logger.log(Level.INFO, "prepareData received");
		// 501
		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}
		if (zip != null) {
			throw new NotImplementedException("the zip parameter is not supported");
		}
		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException("The sessionId parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException("The datasetIds parameter is invalid");
		}
		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException("The datafileIds parameter is invalid");
		}
		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException("At least one of datasetIds or datafileIds parameters must be set");
		}
		if (ValidationHelper.isValidBoolean(compress) == false) {
			throw new BadRequestException("The compress parameter is invalid");
		}
		// at this point we're sure, that all arguments are valid
		logger.log(Level.INFO, "New webservice request: prepareData " + "investigationIds='" + investigationIds + "' "
				+ "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "' " + "compress='" + compress
				+ "' " + "zip='" + zip + "'");

		try {
			requestEntity = requestHelper.createRequest(sessionId, compress, zip, RequestedState.RESTORE_REQUESTED);
			if (datafileIds != null) {
				requestHelper.addDatafiles(sessionId, requestEntity, datafileIds);
			}
			if (datasetIds != null) {
				requestHelper.addDatasets(sessionId, requestEntity, datasetIds);
			}
			// if all the information was restored successfully, we can enqueue requests
			for (Ids2DatafileEntity df : requestEntity.getDatafiles()) {
				this.queue(df, DeferredOp.RESTORE);
			}
			for (Ids2DatasetEntity ds : requestEntity.getDatasets()) {
				this.queue(ds, DeferredOp.RESTORE);
			}
		} catch (ICATSessionException e) {
			throw new ForbiddenException("The sessionId parameter is invalid or has expired");
		} catch (ICATInternalException e) {
			throw new InternalServerErrorException("Unable to connect to ICAT server");
		} catch (PersistenceException e) {
			throw new InternalServerErrorException("Unable to connect to the database");
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(t.getMessage());
		}
		return Response.status(200).entity(requestEntity.getPreparedId() + "\n").build();
//		return Response.status(200).entity(downloadRequestEntity.getPreparedId() + "\n").build(); // IDS1
	}

	/**
	 * Returns the current status of a download request. The current status
	 * values that can be returned are ONLINE and RESTORING.
	 * 
	 * TODO: determine if database connection lost TODO: check if INCOMPLETE
	 * status should be implemented
	 * 
	 * @param preparedId
	 *            The ID of the download request
	 * @return HTTP response containing the current status of the download
	 *         request (ONLINE, IMCOMPLETE, RESTORING, ARCHIVED) Note: only
	 *         ONELINE and RESTORING are implemented
	 */
	public Response getStatus(String preparedId) {
		String status = null;

		// 400
		if (ValidationHelper.isValidId(preparedId) == false) {
			throw new BadRequestException("The preparedId parameter is invalid");
		}

		try {
//			status = downloadRequestHelper.getDownloadRequestByPreparedId(preparedId).getStatus(); // IDS1
			status = requestHelper.getRequestByPreparedId(preparedId).getStatus().name();
		} catch (EJBException e) {
			throw new NotFoundException("No matches found for preparedId \"" + preparedId + "\"");
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(t.getMessage());
		}

		logger.log(Level.INFO, "New webservice request: getStatus " + "preparedId='" + preparedId + "'");

		// convert internal status to appropriate external status
		switch (StatusInfo.valueOf(status)) {
		case SUBMITTED:
		case INFO_RETRIVING:
		case INFO_RETRIVED:
		case RETRIVING:
			status = Status.RESTORING.name();
			break;
		case COMPLETED:
			status = Status.ONLINE.name();
			break;
		case INCOMPLETE:
			status = Status.INCOMPLETE.name();
			break;
		case DENIED:
			throw new ForbiddenException("You do not have permission to download one or more of the requested files");
		case NOT_FOUND:
			throw new NotFoundException("Some of the requested datafile / dataset ids were not found");
		case ERROR:
			throw new InternalServerErrorException("Unable to find files in storage");
		default:
			break;
		}

		return Response.status(200).entity(status + "\n").build();
	}

	/**
	 * Returns a zip file containing the requested files.
	 * 
	 * TODO: find out how to catch the IOException in order to throw 404-file
	 * not longer in cache TODO: work out how to differentiate between NOT FOUND
	 * because of bad preparedId, INCOMPLETE, or some of the requested ids were
	 * not found
	 * 
	 * @param preparedId
	 *            The ID of the download request
	 * @param outname
	 *            The desired filename for the download (optional)
	 * @param offset
	 *            The desired offset of the file (optional)
	 * @return HTTP response containing the ZIP file
	 */
	public Response getData(String preparedId, String outname, String offset) {
		final DownloadRequestEntity downloadRequestEntity;
		final Long offsetLong;
		StreamingOutput strOut = null;
		String name = null;

		// 400
		if (ValidationHelper.isValidId(preparedId) == false) {
			throw new BadRequestException("The preparedId parameter is invalid");
		}

		if (ValidationHelper.isValidName(outname) == false) {
			throw new BadRequestException("The outname parameter is invalid");
		}

		if (ValidationHelper.isValidOffset(offset) == false) {
			throw new BadRequestException("The offset parameter is invalid");
		}

		logger.log(Level.INFO, "New webservice request: getData " + "preparedId='" + preparedId + "' " + "outname='"
				+ outname + "' " + "offset='" + offset + "'");

		try {
			downloadRequestEntity = downloadRequestHelper.getDownloadRequestByPreparedId(preparedId);
		} catch (PersistenceException e) {
			throw new InternalServerErrorException("Unable to connect to the database");
		} catch (EJBException e) {
			throw new NotFoundException("No matches found for preparedId \"" + preparedId + "\"");
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(t.getMessage());
		}

		// the internal download request status must be COMPLETE in order to
		// download the zip
		switch (StatusInfo.valueOf(downloadRequestEntity.getStatus())) {
		case SUBMITTED:
		case INFO_RETRIVING:
		case INFO_RETRIVED:
		case RETRIVING:
			throw new NotFoundException("Requested files are not ready for download");
		case DENIED:
			// TODO: return a list of the 'bad' files?
			throw new ForbiddenException("You do not have permission to download one or more of the requested files");
		case NOT_FOUND:
			// TODO: return list of the 'bad' ids?
			throw new NotFoundException("Some of the requested datafile / dataset ids were not found");
		case INCOMPLETE:
			throw new NotFoundException("Some of the requested files are no longer avaliable in ICAT.");
		case ERROR:
			// TODO: return list of the missing files?
			throw new InternalServerErrorException("Unable to find files in storage");
		case COMPLETED:
		default:
			break;
		}

		// if no outname supplied give default name also suffix with .zip if
		// absent
		if (outname == null) {
			name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(downloadRequestEntity.getSubmittedTime());
			name = name + ".zip";
		} else {
			name = outname;
			String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
			if ("zip".equals(ext) == false) {
				name = name + ".zip";
			}
		}

		// calculate offset
		if (offset != null) {
			offsetLong = Long.parseLong(offset);
		} else {
			offsetLong = 0L;
		}

		File zipFile = new File(PropertyHandler.getInstance().getStorageZipDir() + File.separator
				+ downloadRequestEntity.getPreparedId() + ".zip");

		// if (zipFile.exists() == false) {
		// throw new InternalServerErrorException("The zip file doesn't exist");
		// }
		// if zip file is 0 bytes then something has gone wrong
		if (zipFile.length() == 0) {
			throw new InternalServerErrorException("There was a problem creating the zip file.");
		}

		// check if offset is larger than the file size
		if (offsetLong >= zipFile.length()) {
			System.out.println("\n\n\n\n\n\n" + downloadRequestEntity.getPreparedId() + "\n\n\n\n\n\n\n");
			throw new BadRequestException("Offset (" + offsetLong + " bytes) is larger than file size ("
					+ zipFile.length() + " bytes)");
		}

		// create output stream of the zip file
		strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {
				downloadRequestHelper.writeFileToOutputStream(downloadRequestEntity, output, offsetLong);
			}
		};

		return Response.ok(strOut).header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();
	}

	/**
	 * 
	 * 
	 * TODO: Implement properly for the IDS Reference Implementation!
	 * 
	 * 
	 * 
	 * This method is specifically tailored to the IDS. It will remove files
	 * from the cache that match the userId and any of the dataset or datafile
	 * ids.
	 * 
	 * TODO: try doing better queries -> join with DOWNLOAD_REQUEST and check
	 * for username
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
	 */
	@POST
	@Path("archive")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response archive(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
			@FormParam("datafileIds") String datafileIds) {

		ArrayList<DownloadRequestEntity> matchingDownloadRequests = new ArrayList<DownloadRequestEntity>();
		List<String> idList = null;
		List<DownloadRequestEntity> results = null;

		// 501
		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}

		// 400
		if (ValidationHelper.isValidId(sessionId) == false) {
			throw new BadRequestException("The sessionId parameter is invalid");
		}

		if (ValidationHelper.isValidIdList(datasetIds) == false) {
			throw new BadRequestException("The datasetIds parameter is invalid");
		}

		if (ValidationHelper.isValidIdList(datafileIds) == false) {
			throw new BadRequestException("The datafileIds parameter is invalid");
		}

		if (datasetIds == null && datafileIds == null) {
			throw new BadRequestException("At least one of datasetIds or datafileIds parameters must be set");
		}

		logger.log(Level.INFO, "New webservice request: archive " + "investigationIds='" + investigationIds + "' "
				+ "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		try {
			// find all requests relating to datasetIds
			if (datasetIds != null) {
				idList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
				for (String id : idList) {
					results = downloadRequestHelper.getDownloadRequestFromDatasetId(Long.valueOf(id)); // TODO:
																										// allow
																										// it
																										// to
																										// accept
																										// list
																										// of
																										// Ids?
					matchingDownloadRequests.addAll(results);
				}
			}

			// find all requests relating to datafileIds
			if (datafileIds != null) {
				idList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
				for (String id : idList) {
					results = downloadRequestHelper.getDownloadRequestFromDatafileId(Long.valueOf(id)); // TODO:
																										// allow
																										// it
																										// to
																										// accept
																										// list
																										// of
																										// Ids?
					matchingDownloadRequests.addAll(results);
				}
			}

			// find requests specific to the user and remove
			ICATClientBase client = ICATClientFactory.getInstance().createICATInterface();
			String username = client.getUserId(sessionId);

			for (DownloadRequestEntity dr : matchingDownloadRequests) {
				if (username.equals(dr.getUserid()) == true) {
					downloadRequestHelper.deleteDownloadRequest(dr);
				}
			}
		} catch (ICATSessionException e) {
			throw new ForbiddenException("The sessionId parameter is invalid or has expired");
		} catch (PersistenceException e) {
			throw new InternalServerErrorException("Unable to connect to the database");
		} catch (Exception e) {
			throw new InternalServerErrorException(e.getMessage());
		} catch (Throwable t) {
			throw new InternalServerErrorException(t.getMessage());
		}

		return Response.status(200).entity("").build();
	}

	/*
	 * Unimplemented methods
	 */

	@GET
	@Path("getStatus")
	@Produces("text/plain")
	public Response getStatus(@QueryParam("preparedId") String preparedId, @QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds, @QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafilesIds) {
		Response status = null;
		logger.log(Level.INFO, "received getStatus with preparedId = " + preparedId);
		if (preparedId != null) {
			status = getStatus(preparedId);
		} else {
			status = getStatus(sessionId, investigationIds, datasetIds, datafilesIds);
		}
		return status;
	}

	public Response getStatus(String sessionId, String investigationIds, String datasetIds, String datafilesIds) {
		throw new NotImplementedException("The method 'getStatus(sessionId, investigationIds, datasetIds, "
				+ "datafilesIds)' has not been implemented");
	}

	@GET
	@Path("getData")
	@Produces("application/zip")
	public Response getData(@QueryParam("preparedId") String preparedId, @QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds, @QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds, @QueryParam("compress") String compress,
			@QueryParam("zip") String zip, @QueryParam("outname") String outname, @QueryParam("offset") String offset) {
		Response data = null;
		if (preparedId != null) {
			data = getData(preparedId, outname, offset);
		} else {
			data = getData(sessionId, investigationIds, datasetIds, datafileIds, compress, zip, outname, offset);
		}
		return data;
	}

	public Response getData(String sessionId, String investigationIds, String datasetIds, String datafileIds,
			String compress, String zip, String outname, String offset) {
		throw new NotImplementedException("The method 'getData(sessionId, investigationIds, datasetIds, datafileIds, "
				+ "compress, zip, outname, offset)' has not been implemented");
	}

	@PUT
	@Path("put")
	@Consumes("application/octet-stream")
	public Response put(
			InputStream body, // TODO: Check that this the correct way of
								// handling the data input
			@QueryParam("sessionId") String sessionId, @QueryParam("name") String name,
			@QueryParam("dataFileFormatId") String dataFileFormatId, @QueryParam("datasetId") String datasetId,
			@QueryParam("description") String description, @QueryParam("doi") String doi,
			@QueryParam("datafileCreateTime") String datafileCreateTime,
			@QueryParam("datafileModTime") String datafileModTime) {
		throw new NotImplementedException("The method 'put' has not been implemented");
	}

	@DELETE
	@Path("delete")
	@Produces("text/plain")
	public Response delete(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds, @QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds) {
		throw new NotImplementedException("The method 'delete' has not been implemented");
	}

	@POST
	@Path("restore")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response restore(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
			@FormParam("datafileIds") String datafileIds) {
		throw new NotImplementedException("The method 'restore' has not been implemented");
	}

	private void queue(Ids2DataEntity ds, DeferredOp deferredOp) {
		logger.info("Requesting " + deferredOp + " of " + ds);
		
		Map<Ids2DataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		
		synchronized (deferredOpsQueue) {
			final RequestedState state = deferredOpsQueue.get(ds);
			if (state == null) {
				if (deferredOp == DeferredOp.WRITE) {
					deferredOpsQueue.put(ds, RequestedState.WRITE_REQUESTED);
					this.setDelay(ds);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(ds, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(ds, RequestedState.RESTORE_REQUESTED);
				}
			}
			else {
				// if we are overwriting a DS from a different request, we should
				// remove this old DS from its request, as it will never be COMPLETED
//				for (Ids2DataEntity oldDs : deferredOpsQueue.keySet()) {
//					if (oldDs.equals(ds)) {
//						oldDs.getRequest().getDatasets().remove(oldDs);
//						break;
//					}
//				}
				
				if (state == RequestedState.ARCHIVE_REQUESTED) {
					if (deferredOp == DeferredOp.WRITE) {
						deferredOpsQueue.put(ds, RequestedState.WRITE_REQUESTED);
						this.setDelay(ds);
					} else if (deferredOp == DeferredOp.RESTORE) {
						deferredOpsQueue.put(ds, RequestedState.RESTORE_REQUESTED);
					}
				} else if (state == RequestedState.RESTORE_REQUESTED) {
					if (deferredOp == DeferredOp.WRITE) {
						deferredOpsQueue.put(ds, RequestedState.WRITE_REQUESTED);
						this.setDelay(ds);
					} else if (deferredOp == DeferredOp.ARCHIVE) {
						deferredOpsQueue.put(ds, RequestedState.ARCHIVE_REQUESTED);
					}
				} else if (state == RequestedState.WRITE_REQUESTED) {
					if (deferredOp == DeferredOp.WRITE) {
						this.setDelay(ds);
					} else if (deferredOp == DeferredOp.ARCHIVE) {
						deferredOpsQueue.put(ds, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
					}
				} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
					if (deferredOp == DeferredOp.WRITE) {
						this.setDelay(ds);
					} else if (deferredOp == DeferredOp.RESTORE) {
						deferredOpsQueue.put(ds, RequestedState.WRITE_REQUESTED);
					}
				}
			}
			
		}
	}

	private void setDelay(Ids2DataEntity de) {
		Map<Ids2DataEntity, Long> writeTimes = requestQueues.getWriteTimes();
		
		writeTimes.put(de, System.currentTimeMillis() + this.archiveWriteDelayMillis);
		final Date d = new Date(writeTimes.get(de));
		logger.info("Requesting delay of writing of " + de + " till " + d);
	}
}