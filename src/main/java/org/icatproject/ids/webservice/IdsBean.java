package org.icatproject.ids.webservice;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
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
import org.icatproject.ids.thread.Preparer;
import org.icatproject.ids.thread.Preparer.PreparerStatus;
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
				fsm.queue(dsInfo, DeferredOp.ARCHIVE);
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
				datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		Status status = Status.ONLINE;
		DataNotOnlineException exc = null;

		Collection<DsInfo> dsInfos = dataSelection.getDsInfo();
		if (twoLevel) {
			try {
				for (DsInfo dsInfo : dsInfos) {
					if (!mainStorage.exists(dsInfo)) {
						// TODO include Status.Restoring
						status = Status.ARCHIVED;
						fsm.queue(dsInfo, DeferredOp.RESTORE);
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

		// Remove the local data set cache
		try {
			for (DsInfo dsInfo : dsInfos) {
				Files.deleteIfExists(datasetDir.resolve(dsInfo.getFacilityName())
						.resolve(dsInfo.getInvName()).resolve(dsInfo.getVisitId())
						.resolve(dsInfo.getDsName()));
			}
			for (DatafileInfo dfInfo : dataSelection.getDfInfo()) {
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
		return Response.ok().build();
	}

	public Response getData(String preparedId, String outname, final long offset)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException {

		// Log and validate
		logger.info("New webservice request: getData preparedId = '" + preparedId + "' outname = '"
				+ outname + "' offset = " + offset);

		validateUUID("sessionId", preparedId);

		// Do it
		String name;

		// Determine path and name
		Path path = preparedDir.resolve(preparedId);
		if (Files.isRegularFile(path)) {
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
		} else {
			if (Files.isDirectory(path)) {
				path = path.toFile().listFiles()[0].toPath();
				if (outname == null) {
					name = path.getFileName().toString();
				} else {
					name = outname;
				}
			} else {
				Preparer preparer = fsm.getPreparer(preparedId);
				if (preparer == null) {
					throw new NotFoundException("The preparedId " + preparedId + " is not known");
				}
				PreparerStatus status = preparer.getStatus();
				if (status == PreparerStatus.COMPLETE) {
					throw new NotFoundException("File was prepared but is no longer available");
				} else if (status == PreparerStatus.INCOMPLETE) {
					throw new NotFoundException(preparer.getMessage());
				} else {
					throw new NotFoundException("File is not yet ready");
				}
			}
		}
		final Path finalPath = path;
		logger.debug(path + " " + name);

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {
				if (offset != 0) { // Wrap if needed
					output = new RangeOutputStream(output, offset, null);
				}
				Files.copy(finalPath, output);
			}
		};
		return Response
				.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
				.entity(strOut)
				.header("Content-Disposition", "attachment; filename=\"" + name + "\"")
				.header("Accept-Ranges", "bytes").build();
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
						fsm.queue(dsInfo, DeferredOp.RESTORE);
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

		final boolean finalZip = zip ? true : dataSelection.mustZip();

		StreamingOutput strOut = new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException {

				if (offset != 0) { // Wrap if needed
					output = new RangeOutputStream(output, offset, null);
				}

				byte[] bytes = new byte[BUFSIZ];
				if (finalZip) {
					ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(output));
					if (!compress) {
						zos.setLevel(0); // Otherwise use default compression
					}

					for (DatafileInfo dfInfo : dataSelection.getDfInfo()) {
						zos.putNextEntry(new ZipEntry("ids/" + dfInfo.getDfLocation()));
						InputStream stream = mainStorage.get(dfInfo.getDfLocation());

						int length;
						while ((length = stream.read(bytes)) >= 0) {
							zos.write(bytes, 0, length);
						}
						zos.closeEntry();
					}
					zos.close();
				} else {
					InputStream stream = mainStorage.get(dataSelection.getDfInfo().iterator()
							.next().getDfLocation());
					int length;
					while ((length = stream.read(bytes)) >= 0) {
						output.write(bytes, 0, length);
					}
					output.close();
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

	public Response getStatus(String preparedId) throws BadRequestException, NotFoundException,
			InternalException, InsufficientPrivilegesException, NotImplementedException {

		// Log and validate
		logger.info("New webservice request: getStatus " + "preparedId='" + preparedId + "'");

		validateUUID("preparedId", preparedId);

		// Do it
		Status status = Status.ONLINE;
		final Path path = preparedDir.resolve(preparedId);
		if (!Files.exists(path)) {
			Preparer preparer = fsm.getPreparer(preparedId);
			if (preparer == null) {
				throw new NotFoundException("The preparedId " + preparedId + " is not known");
			}
			PreparerStatus preparerStatus = preparer.getStatus();
			if (preparerStatus == PreparerStatus.COMPLETE) {
				throw new NotFoundException("ZIP file was prepared but is no longer available");
			} else if (preparerStatus == PreparerStatus.INCOMPLETE) {
				throw new NotFoundException(preparer.getMessage());
			} else {
				status = Status.RESTORING;
			}
		}

		return Response.ok(status.name()).build();

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

	@PostConstruct
	private void init() throws DatatypeConfigurationException, IOException {
		logger.info("creating IdsBean");
		propertyHandler = PropertyHandler.getInstance();
		mainStorage = propertyHandler.getMainStorage();
		twoLevel = propertyHandler.getArchiveStorage() != null;
		datatypeFactory = DatatypeFactory.newInstance();
		preparedDir = propertyHandler.getCacheDir().resolve("prepared");
		Files.createDirectories(preparedDir);
		datasetDir = propertyHandler.getCacheDir().resolve("dataset");
		Files.createDirectories(datasetDir);

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

		final DataSelection dataSelection = new DataSelection(icat, sessionId, investigationIds,
				datasetIds, datafileIds, Returns.DATASETS_AND_DATAFILES);

		// Do it
		String preparedId = UUID.randomUUID().toString();
		Preparer preparer = new Preparer(preparedId, dataSelection, propertyHandler, fsm,
				compress, zip ? true : dataSelection.mustZip());
		new Thread(preparer).start();
		fsm.registerPreparer(preparedId, preparer);

		return Response.ok().entity(preparedId).build();
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
					fsm.queue(dsInfo, DeferredOp.RESTORE);
					throw new DataNotOnlineException(
							"Before putting a datafile, its dataset has to be restored, restoration requested automatically");
				}
			}

			// Remove the local data set cache
			Files.deleteIfExists(datasetDir.resolve(dsInfo.getFacilityName())
					.resolve(dsInfo.getInvName()).resolve(dsInfo.getVisitId())
					.resolve(dsInfo.getDsName()));

			DfInfo dfInfo = mainStorage.put(dsInfo, name, body);
			Long dfId = registerDatafile(sessionId, name, datafileFormatId, dfInfo, ds,
					description, doi, datafileCreateTime, datafileModTime);

			if (twoLevel) {
				fsm.queue(dsInfo, DeferredOp.WRITE);
			}

			return Response.status(HttpURLConnection.HTTP_CREATED).entity(Long.toString(dfId))
					.build();

		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
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
				fsm.queue(dsInfo, DeferredOp.RESTORE);
			}
		}

		return Response.ok().build();
	}

	public Response getServiceStatus(String sessionId) throws InternalException {

		// Log and validate
		logger.info("New webservice request: getServiceStatus");

		return Response.ok(fsm.getServiceStatus()).build();
	}

}
