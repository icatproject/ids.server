package org.icatproject.ids.util;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ICAT;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.entity.IdsDatafileEntity;
import org.icatproject.ids.entity.IdsDatasetEntity;
import org.icatproject.ids.entity.IdsRequestEntity;
import org.icatproject.ids.webservice.DeferredOp;
import org.icatproject.ids.webservice.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.webservice.exceptions.InternalException;
import org.icatproject.ids.webservice.exceptions.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stateless
public class RequestHelper {
	private final static boolean DEFAULT_COMPRESS = false;
	private final static boolean DEFAULT_ZIP = false;
	private final static Logger logger = LoggerFactory.getLogger(RequestHelper.class);
	private PropertyHandler properties = PropertyHandler.getInstance();

	@PersistenceContext(unitName = "IDS-PU")
	private EntityManager em;

	private ICAT icat;

	@PostConstruct
	private void init() {
		icat = PropertyHandler.getInstance().getIcatService();
	}

	public IdsRequestEntity createPrepareRequest(String sessionId, boolean compress, boolean zip)
			throws InsufficientPrivilegesException, InternalException {
		return createRequest(sessionId, compress, zip, DeferredOp.PREPARE);
	}

	public IdsRequestEntity createArchiveRequest(String sessionId)
			throws InsufficientPrivilegesException, InternalException {
		return createRequest(sessionId, DEFAULT_COMPRESS, DEFAULT_ZIP, DeferredOp.ARCHIVE);
	}

	public IdsRequestEntity createRestoreRequest(String sessionId)
			throws InsufficientPrivilegesException, InternalException {
		return createRequest(sessionId, DEFAULT_COMPRESS, DEFAULT_ZIP, DeferredOp.RESTORE);
	}

	public IdsRequestEntity createWriteRequest(String sessionId)
			throws InsufficientPrivilegesException, InternalException {
		return createRequest(sessionId, DEFAULT_COMPRESS, DEFAULT_ZIP, DeferredOp.WRITE);
	}

	private IdsRequestEntity createRequest(String sessionId, boolean compress, boolean zip,
			DeferredOp deferredOp) throws InsufficientPrivilegesException, InternalException {
		Calendar expireDate = Calendar.getInstance();
		expireDate.add(Calendar.DATE, properties.getRequestExpireTimeDays());

		String username;
		try {
			username = icat.getUserName(sessionId);
		} catch (IcatException_Exception e) {
			IcatExceptionType type = e.getFaultInfo().getType();
			if (type == IcatExceptionType.SESSION) {
				throw new InsufficientPrivilegesException(e.getMessage());
			}
			throw new InternalException(type + " " + e.getMessage());
		}

		IdsRequestEntity requestEntity = new IdsRequestEntity();
		requestEntity.setSessionId(sessionId);
		requestEntity.setUserId(username);
		requestEntity.setPreparedId(UUID.randomUUID().toString());
		requestEntity.setStatus(StatusInfo.SUBMITTED);
		requestEntity.setCompress(compress);
		requestEntity.setSubmittedTime(new Date());
		requestEntity.setExpireTime(expireDate.getTime());
		requestEntity.setDeferredOp(deferredOp);

		em.persist(requestEntity);

		return requestEntity;
	}

	public void addDatasets(String sessionId, IdsRequestEntity requestEntity,
			List<Long> datasetIdList) throws IcatException_Exception {

		for (long id : datasetIdList) {
			Dataset ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", id);
			addDataset(sessionId, requestEntity, ds);
		}
	}

	public void addDataset(String sessionId, IdsRequestEntity requestEntity, long id)
			throws InsufficientPrivilegesException, InternalException, NotFoundException {
		Dataset ds;
		try {
			ds = (Dataset) icat.get(sessionId, "Dataset INCLUDE Datafile", id);
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
		addDataset(sessionId, requestEntity, ds);
	}

	public void addDataset(String sessionId, IdsRequestEntity requestEntity, Dataset dataset) {
		IdsDatasetEntity newDataset = new IdsDatasetEntity();
		newDataset.setIcatDatasetId(dataset.getId());
		newDataset.setIcatDataset(dataset);
		newDataset.setRequest(requestEntity);
		newDataset.setStatus(StatusInfo.SUBMITTED);
		newDataset.setLocation(dataset.getLocation());
		em.persist(newDataset);
		requestEntity.getDatasets().add(newDataset);
	}

	public void addDatafiles(String sessionId, IdsRequestEntity requestEntity,
			List<Long> datafileIdList) throws NumberFormatException, IcatException_Exception {

		for (Long id : datafileIdList) {
			Datafile df = (Datafile) icat.get(sessionId, "Datafile INCLUDE Dataset", id);
			addDatafile(sessionId, requestEntity, df);
		}
	}

	public void addDatafile(String sessionId, IdsRequestEntity requestEntity, Datafile datafile) {
		IdsDatafileEntity newDatafile = new IdsDatafileEntity();
		newDatafile.setIcatDatafileId(datafile.getId());
		newDatafile.setIcatDatafile(datafile);
		newDatafile.setRequest(requestEntity);
		newDatafile.setStatus(StatusInfo.SUBMITTED);
		newDatafile.setLocation(datafile.getLocation());
		em.persist(newDatafile);
		requestEntity.getDatafiles().add(newDatafile);
	}

	public void setDataEntityStatus(IdsDataEntity de, StatusInfo status) {
		logger.info("Changing status of " + de + " to " + status);
		de.setStatus(status);
		setRequestCompletedIfEverythingDone(de.getRequest());
		em.merge(de);
	}

	private void setRequestCompletedIfEverythingDone(IdsRequestEntity request) {
		Set<StatusInfo> finalStatuses = new HashSet<StatusInfo>();
		finalStatuses.add(StatusInfo.COMPLETED);
		finalStatuses.add(StatusInfo.INCOMPLETE);

		// assuming that everything went OK
		StatusInfo resultingRequestStatus = StatusInfo.COMPLETED;
		logger.info(String.format("Will check status of %s data entities (%s DS, %s DF)", request
				.getDataEntities().size(), request.getDatasets().size(), request.getDatafiles()
				.size()));
		for (IdsDataEntity de : request.getDataEntities()) {
			logger.info("Status of " + de + " is " + de.getStatus());
			if (!finalStatuses.contains(de.getStatus())) {
				return;
			}
			if (de.getStatus() != StatusInfo.COMPLETED) {
				resultingRequestStatus = StatusInfo.INCOMPLETE;
				break;
			}
		}
		logger.info("all tasks in request " + request + " finished");
		setRequestStatus(request, resultingRequestStatus);
	}

	public void setRequestStatus(IdsRequestEntity request, StatusInfo status) {
		logger.info("Changing status of " + request + " to " + status);
		request.setStatus(status);
		em.merge(request);
	}

	public List<IdsRequestEntity> getRequestByPreparedId(String preparedId) {
		return em.createNamedQuery("findRequestByPreparedId", IdsRequestEntity.class)
				.setParameter("preparedId", preparedId).getResultList();
	}

	public List<IdsRequestEntity> getUnfinishedRequests() {
		Query q = em.createNamedQuery("findUnfinishedRequests"); // TODO untested, was explicit
																	// query before
		@SuppressWarnings("unchecked")
		List<IdsRequestEntity> requests = (List<IdsRequestEntity>) q.getResultList();
		logger.info("Found " + requests.size() + " unfinished requests");
		Iterator<IdsRequestEntity> it = requests.iterator();
		while (it.hasNext()) {
			IdsRequestEntity request = it.next();
			try {
				for (IdsDatafileEntity df : request.getDatafiles()) {
					df.setIcatDatafile((Datafile) icat.get(request.getSessionId(),
							"Datafile INCLUDE Dataset", df.getIcatDatafileId()));
				}
				for (IdsDatasetEntity ds : request.getDatasets()) {
					ds.setIcatDataset((Dataset) icat.get(request.getSessionId(),
							"Dataset INCLUDE Datafile", ds.getIcatDatasetId()));
				}
			} catch (IcatException_Exception e) {
				logger.warn("Couldn't resume processing " + request);
				setRequestStatus(request, StatusInfo.INCOMPLETE);
				it.remove();
			}
		}
		logger.info(requests.size() + " unfinished requests ready to be resumed");
		return requests;
	}
}
