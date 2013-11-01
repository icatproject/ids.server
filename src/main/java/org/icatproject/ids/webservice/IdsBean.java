package org.icatproject.ids.webservice;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.icatproject.Datafile;
import org.icatproject.DatafileFormat;
import org.icatproject.Dataset;
import org.icatproject.EntityBaseBean;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.MainStorageInterface.DfInfo;
import org.icatproject.ids.thread.Writer;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.RangeOutputStream;
import org.icatproject.ids.webservice.DataSelection.DatafileInfo;
import org.icatproject.ids.webservice.DataSelection.Returns;
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

	/** matches standard UUID format of 8-4-4-4-12 hexadecimal digits */
	public static final Pattern uuidRegExp = Pattern
			.compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

	private long archiveWriteDelayMillis = PropertyHandler.getInstance().getWriteDelaySeconds() * 1000L;

	private DatatypeFactory datatypeFactory;

	private MainStorageInterface mainStorage;

	private Timer timer = new Timer();

	private ICAT icat;

	private boolean twoLevel;

	private long processQueueIntervalMillis;

	private PropertyHandler propertyHandler;

	public Response archive(String sessionId, String investigationIds, String datasetIds,
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
			// TODO don't archive data if the ds is contributing to being prepared
			Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos) {
				this.queue(dsInfo, DeferredOp.ARCHIVE);
			}
		}

		return Response.ok().build();

	}

	public Response delete(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, InternalException, NotFoundException,
			DataNotOnlineException {

		logger.info("New webservice request: delete " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		IdsBean.validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS);

		// Do it
		Status status = Status.ONLINE;
		DataNotOnlineException exc = null;

		if (twoLevel) {
			try {
				Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
				for (DsInfo dsInfo : dsInfos) {
					if (!mainStorage.exists(dsInfo)) {
						// TODO include Status.Restoring
						status = Status.ARCHIVED;
						this.queue(dsInfo, DeferredOp.RESTORE);
						exc = new DataNotOnlineException("Current status of Dataset "
								+ dsInfo.getDsId() + " is ARCHIVED. It is now being restored.");
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		if (exc != null) {
			throw exc;
		}

		/* Now delete from ICAT */
		List<EntityBaseBean> dfs = new ArrayList<>();
		for (DatafileInfo dfInfo : dataSelection.getDfInfo()) {
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

		for (DatafileInfo dfInfo : dataSelection.getDfInfo()) {
			try {
				mainStorage.delete(dfInfo.getDfLocation());
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		if (twoLevel) {
			Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos) {
				this.queue(dsInfo, DeferredOp.WRITE);
			}
		}
		return Response.ok().build();
	}

	public Response getData(String preparedId, String outname, final long offset)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException {

		// final IdsRequestEntity requestEntity;
		// String name = null;
		//
		// IdsBean.validateUUID("preparedId", preparedId);
		//
		// List<IdsRequestEntity> requests = requestHelper.getRequestByPreparedId(preparedId);
		// if (requests.size() == 0) {
		// throw new NotFoundException("No matches found for preparedId \"" + preparedId + "\"");
		// }
		// if (requests.size() > 1) {
		// String msg = "More than one match found for preparedId \"" + preparedId + "\"";
		// logger.error(msg);
		// throw new InternalException(msg);
		// }
		// requestEntity = requests.get(0);
		//
		// StatusInfo statusInfo = requestEntity.getStatus();
		// Status status = statusInfo.getUserStatus();
		// if (status == null) {
		// if (statusInfo == StatusInfo.DENIED) {
		// throw new InsufficientPrivilegesException(
		// "You do not have permission to download one or more of the requested files");
		// } else if (statusInfo == StatusInfo.NOT_FOUND) {
		// throw new NotFoundException("Some of the requested data ids were not found");
		// } else if (statusInfo == StatusInfo.ERROR) {
		// String msg = "Unable to find files in storage";
		// logger.error(msg);
		// throw new InternalException(msg);
		// }
		// } else if (status == Status.INCOMPLETE) {
		// throw new NotFoundException(
		// "Some of the requested files are no longer available in ICAT.");
		// } else if (status == Status.RESTORING) {
		// throw new NotFoundException("Requested files are not ready for download");
		// }
		//
		// /* if no outname supplied give default name also suffix with .zip if absent */
		// if (outname == null) {
		// name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(requestEntity
		// .getSubmittedTime());
		// name = name + ".zip";
		// } else {
		// name = outname;
		// String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
		// if ("zip".equals(ext) == false) {
		// name = name + ".zip";
		// }
		// }
		// logger.debug("Outname set to " + name);
		//
		// StreamingOutput strOut = new StreamingOutput() {
		// @Override
		// public void write(OutputStream output) throws IOException {
		// try {
		// InputStream input = mainStorage.getPreparedZip(requestEntity.getPreparedId()
		// + ".zip");
		// logger.debug("Requested offset " + offset);
		// long skipped = 0;
		// try {
		// skipped = input.skip(offset);
		// logger.debug("Skipped " + skipped);
		// } catch (IOException e) {
		// throw new WebApplicationException(Response
		// .status(HttpURLConnection.HTTP_BAD_REQUEST)
		// .entity(e.getClass() + " " + e.getMessage()).build());
		// }
		// if (skipped != offset) {
		// throw new WebApplicationException(Response
		// .status(HttpURLConnection.HTTP_BAD_REQUEST)
		// .entity("Offset (" + offset + " bytes) is larger than file size ("
		// + skipped + " bytes)").build());
		// }
		// copy(input, output);
		// } catch (IOException e) {
		// throw new WebApplicationException(Response
		// .status(HttpURLConnection.HTTP_INTERNAL_ERROR)
		// .entity(e.getClass() + " " + e.getMessage()).build());
		// }
		// }
		// };
		// return Response
		// .status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
		// .entity(strOut)
		// .header("Content-Disposition", "attachment; filename=\"" + name + "\"")
		// .header("Accept-Ranges", "bytes").build();
		return null;
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
		Status status = Status.ONLINE;
		DataNotOnlineException exc = null;

		if (twoLevel) {
			try {
				Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
				for (DsInfo dsInfo : dsInfos) {
					if (!mainStorage.exists(dsInfo)) {
						// TODO include Status.Restoring
						status = Status.ARCHIVED;
						this.queue(dsInfo, DeferredOp.RESTORE);
						exc = new DataNotOnlineException("Current status of Dataset "
								+ dsInfo.getDsId() + " is ARCHIVED. It is now being restored.");
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}
		}

		if (exc != null) {
			throw exc;
		}

		logger.debug("About to return data");

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {

				if (offset != 0) { // Wrap if needed
					output = new RangeOutputStream(output, offset, null);
				}

				ZipOutputStream zos = new ZipOutputStream(output);
				if (!compress) {
					zos.setLevel(0); // Otherwise use default compression
				}

				for (DatafileInfo dfInfo : dataSelection.getDfInfo()) {
					zos.putNextEntry(new ZipEntry("ids/" + dfInfo.getDfLocation()));
					InputStream stream = mainStorage.get(dfInfo.getDfLocation());
					byte[] bytes = new byte[BUFSIZ];
					int length;
					while ((length = stream.read(bytes)) >= 0) {
						zos.write(bytes, 0, length);
					}
					zos.closeEntry();
				}
				zos.close();
			}
		};

		/* Construct the name to include in the headers */
		String name;
		if (outname == null) {
			name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) + ".zip";
		} else {
			String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
			if ("zip".equals(ext)) {
				name = outname;
			} else {
				name = outname + ".zip";
			}
		}
		return Response
				.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();

	}

	public Response getStatus(String preparedId) throws BadRequestException, NotFoundException,
			InternalException, InsufficientPrivilegesException, NotImplementedException {

		logger.info("New webservice request: getStatus " + "preparedId='" + preparedId + "'");

		IdsBean.validateUUID("preparedId", preparedId);

		// List<IdsRequestEntity> requests = requestHelper.getRequestByPreparedId(preparedId);
		// if (requests.size() == 0) {
		// throw new NotFoundException("No matches found for preparedId \"" + preparedId + "\"");
		// }
		// if (requests.size() > 1) {
		// String msg = "More than one match found for preparedId \"" + preparedId + "\"";
		// logger.error(msg);
		// throw new InternalException(msg);
		// }
		// StatusInfo statusInfo = requests.get(0).getStatus();
		//
		// Status status = statusInfo.getUserStatus();
		// if (status == null) {
		// if (statusInfo == StatusInfo.DENIED) {
		// throw new InsufficientPrivilegesException(
		// "You do not have permission to download one or more of the requested files");
		// } else if (statusInfo == StatusInfo.NOT_FOUND) {
		// throw new NotFoundException(
		// "Some of the requested datafile / dataset ids were not found");
		// } else if (statusInfo == StatusInfo.ERROR) {
		// throw new InternalException("Unable to find files in storage");
		// }
		// }
		// return Response.ok(status.name()).build();
		return null;

	}

	public Response getStatus(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException {

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
				Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
				for (DsInfo dsInfo : dsInfos) {
					if (!mainStorage.exists(dsInfo)) {
						// TODO include Status.Restoring (which does not break;)
						status = Status.ARCHIVED;
						break;
					}
				}
			} catch (IOException e) {
				throw new InternalException(e.getClass() + " " + e.getMessage());
			}

		}
		logger.debug("Status is " + status.name());
		return Response.ok(status.name()).build();

	}

	private class ProcessQueue extends TimerTask {

		@Override
		public void run() {
			try {
				synchronized (deferredOpsQueue) {
					final long now = System.currentTimeMillis();
					final Iterator<Entry<DsInfo, RequestedState>> it = deferredOpsQueue.entrySet()
							.iterator();
					while (it.hasNext()) {
						final Entry<DsInfo, RequestedState> opEntry = it.next();
						final DsInfo dsInfo = opEntry.getKey();
						if (!changing.contains(dsInfo)) {
							final RequestedState state = opEntry.getValue();
							logger.debug("Will process " + dsInfo + " with " + state);
							if (state == RequestedState.WRITE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									writeTimes.remove(dsInfo);
									changing.add(dsInfo);
									it.remove();
									final Thread w = new Thread(new Writer(dsInfo, propertyHandler,
											deferredOpsQueue, changing));
									w.start();
								}
							} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									writeTimes.remove(dsInfo);
									changing.add(dsInfo);
									it.remove();
									// final Thread w = new Thread(new WriteThenArchiver(dsInfo,
									// propertyHandler, deferredOpsQueue, changing));
									// w.start();
								}
							} else if (state == RequestedState.ARCHIVE_REQUESTED) {
								changing.add(dsInfo);
								it.remove();
								final Thread w = new Thread(
										new org.icatproject.ids.thread.Archiver(dsInfo,
												propertyHandler, deferredOpsQueue, changing));
								w.start();
							} else if (state == RequestedState.RESTORE_REQUESTED) {
								changing.add(dsInfo);
								it.remove();
								// final Thread w = new Thread(new Restorer(dsInfo, propertyHandler,
								// deferredOpsQueue, changing));
								// w.start();
							}
						}
					}
				}
			} finally {
				timer.schedule(new ProcessQueue(), processQueueIntervalMillis);
			}

		}
	}

	@PostConstruct
	private void init() throws DatatypeConfigurationException {
		logger.info("creating IdsBean");
		propertyHandler = PropertyHandler.getInstance();
		mainStorage = propertyHandler.getMainStorage();
		twoLevel = propertyHandler.getArchiveStorage() != null;
		datatypeFactory = DatatypeFactory.newInstance();
		if (propertyHandler.getProcessQueueIntervalSeconds() != 0) {
			processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;
			timer.schedule(new ProcessQueue(), processQueueIntervalMillis);
		}
		icat = propertyHandler.getIcatService();

		restartUnfinishedWork();

		logger.info("created IdsBean");
	}

	public Response prepareData(String sessionId, String investigationIds, String datasetIds,
			String datafileIds, boolean compress, boolean zip) throws NotImplementedException,
			BadRequestException, InternalException, InsufficientPrivilegesException,
			NotFoundException {

		// Log and validate
		logger.info("New webservice request: prepareData " + "investigationIds='"
				+ investigationIds + "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='"
				+ datafileIds + "' " + "compress='" + compress + "' " + "zip='" + zip + "'");

		validateUUID("sessionId", sessionId);

		// Do it
		// IdsRequestEntity requestEntity = null;
		// try {
		// requestEntity = requestHelper.createPrepareRequest(sessionId, compress, zip);
		// requestHelper.addDatafiles(sessionId, requestEntity, dfids);
		// requestHelper.addDatasets(sessionId, requestEntity, dsids);
		// for (IdsDataEntity de : requestEntity.getDataEntities()) {
		// this.queue(de, DeferredOp.PREPARE);
		// final Thread w = new Thread(new Preparer(de, requestHelper));
		// w.start();
		// }
		// } catch (IcatException_Exception e) {
		// IcatExceptionType type = e.getFaultInfo().getType();
		// if (type == IcatExceptionType.INSUFFICIENT_PRIVILEGES
		// || type == IcatExceptionType.SESSION) {
		// throw new InsufficientPrivilegesException(e.getMessage());
		// }
		// if (type == IcatExceptionType.NO_SUCH_OBJECT_FOUND) {
		// throw new NotFoundException(e.getMessage());
		// }
		// throw new InternalException(type + " " + e.getMessage());
		//
		// } catch (RuntimeException e) {
		// processRuntimeException(e);
		// }
		// return Response.status(200).entity(requestEntity.getPreparedId()).build();
		return null;
	}

	public Response put(InputStream body, String sessionId, String name, long datafileFormatId,
			long datasetId, String description, String doi, Long datafileCreateTime,
			Long datafileModTime) throws NotFoundException, DataNotOnlineException,
			BadRequestException, InsufficientPrivilegesException, InternalException {

		// Log and validate
		logger.info("New webservice request: put " + "name='" + name + "' " + "datafileFormatId='"
				+ datafileFormatId + "' " + "datasetId='" + datasetId + "' " + "description='"
				+ description + "' " + "doi='" + doi + "' " + "datafileCreateTime='"
				+ datafileCreateTime + "' " + "datafileModTime='" + datafileModTime + "'");

		IdsBean.validateUUID("sessionId", sessionId);
		if (name == null) {
			throw new BadRequestException("The name parameter must be set");
		}

		// Do it
		Dataset ds;
		try {
			ds = (Dataset) icat
					.get(sessionId, "Dataset INCLUDE Investigation, Facility", datasetId);
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

		DsInfo dsInfo = new DsInfoImpl(ds, icat, sessionId);
		try {

			if (twoLevel) {
				if (!mainStorage.exists(dsInfo)) {
					this.queue(dsInfo, DeferredOp.RESTORE);
					throw new DataNotOnlineException(
							"Before putting a datafile, its dataset has to be restored, restoration requested automatically");
				}
			}

			DfInfo dfInfo = mainStorage.put(dsInfo, name, body);
			Long dfId = registerDatafile(sessionId, name, datafileFormatId, dfInfo, ds,
					description, doi, datafileCreateTime, datafileModTime);

			if (twoLevel) {
				queue(dsInfo, DeferredOp.WRITE);
			}

			return Response.status(HttpURLConnection.HTTP_CREATED).entity(Long.toString(dfId))
					.build();

		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}

	}

	private final Map<DsInfo, Long> writeTimes = new HashMap<>();

	private final Set<DsInfo> changing = new HashSet<>();
	private final Map<DsInfo, RequestedState> deferredOpsQueue = new HashMap<>();

	public enum RequestedState {
		ARCHIVE_REQUESTED, RESTORE_REQUESTED, WRITE_REQUESTED, WRITE_THEN_ARCHIVE_REQUESTED
	}

	private void queue(DsInfo dsInfo, DeferredOp deferredOp) {
		logger.info("Requesting " + deferredOp + " of " + dsInfo);

		synchronized (deferredOpsQueue) {

			final RequestedState state = this.deferredOpsQueue.get(dsInfo);
			if (state == null) {
				if (deferredOp == DeferredOp.WRITE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.RESTORE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
				}
			}
		}
	}

	private Long registerDatafile(String sessionId, String name, long datafileFormatId,
			DfInfo dfInfo, Dataset dataset, String description, String doi,
			Long datafileCreateTime, Long datafileModTime) throws InsufficientPrivilegesException,
			NotFoundException, InternalException, BadRequestException {
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
		df.setLocation(dfInfo.getLocation());
		df.setFileSize(dfInfo.getSize());
		df.setChecksum(Long.toHexString(dfInfo.getChecksum()));
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
				+ dfInfo.getLocation());
		return df.getId();
	}

	private void restartUnfinishedWork() {
		// List<IdsRequestEntity> requests = requestHelper.getUnfinishedRequests();
		// for (IdsRequestEntity request : requests) {
		// for (IdsDataEntity de : request.getDataEntities()) {
		// queue(de, request.getDeferredOp());
		// }
		// }
	}

	public Response restore(String sessionId, String investigationIds, String datasetIds,
			String datafileIds) throws BadRequestException, NotImplementedException,
			InsufficientPrivilegesException, InternalException, NotFoundException {

		// Log and validate
		logger.info("New webservice request: restore " + "investigationIds='" + investigationIds
				+ "' " + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

		if (investigationIds != null) {
			throw new NotImplementedException("investigationIds are not supported");
		}

		validateUUID("sessionId", sessionId);

		DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS);

		// Do it

		if (twoLevel) {
			Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
			for (DsInfo dsInfo : dsInfos) {
				this.queue(dsInfo, DeferredOp.RESTORE);
			}
		}

		return Response.ok().build();
	}

	private void setDelay(DsInfo dsInfo) {
		writeTimes.put(dsInfo, System.currentTimeMillis() + archiveWriteDelayMillis);
		if (logger.isDebugEnabled()) {
			final Date d = new Date(writeTimes.get(dsInfo));
			logger.debug("Requesting delay of writing of " + dsInfo + " till " + d);
		}
	}

	public static void validateUUID(String thing, String id) throws BadRequestException {
		if (id == null || !uuidRegExp.matcher(id).matches())
			throw new BadRequestException("The " + thing + " parameter '" + id
					+ "' is not a valid UUID");
	}

}
