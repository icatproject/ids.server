package org.icatproject.ids2.ported;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.icatproject.Dataset;
import org.icatproject.ids.icatclient.ICATClientBase;
import org.icatproject.ids.icatclient.ICATClientFactory;
import org.icatproject.ids.icatclient.exceptions.ICATClientException;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;

@Stateless
public class RequestHelper {

	private final static Logger logger = Logger.getLogger(RequestHelper.class.getName());
	private PropertyHandler properties = PropertyHandler.getInstance();
//	private ICATClientBase icatClient;

	@PersistenceContext(unitName = "IDS-PU")
	private EntityManager em;
	
//	@PostConstruct
//	public void postConstruct() throws MalformedURLException, ICATClientException {
//		icatClient = ICATClientFactory.getInstance().createICATInterface();
//	}

	public RequestEntity createRequest(String sessionId, String compress, String zip, RequestedState requestedState)
			throws ICATClientException, MalformedURLException {
		ICATClientBase client = ICATClientFactory.getInstance().createICATInterface();
		Calendar expireDate = Calendar.getInstance();
		expireDate.add(Calendar.DATE, properties.getNumberOfDaysToExpire());

		String username = client.getUserId(sessionId);

		RequestEntity requestEntity = new RequestEntity();
		requestEntity.setSessionId(sessionId);
		requestEntity.setUserId(username);
		requestEntity.setPreparedId(UUID.randomUUID().toString());
		requestEntity.setStatus(StatusInfo.SUBMITTED);
		requestEntity.setCompress(Boolean.parseBoolean(compress));
		requestEntity.setSubmittedTime(new Date());
		requestEntity.setExpireTime(expireDate.getTime());
		requestEntity.setRequestedState(requestedState);

		em.persist(requestEntity);
		em.flush();

		return requestEntity;
	}
	
	public void addDatasets(String sessionId, RequestEntity requestEntity, String datasetIds) throws Exception {
        List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
        List<Dataset> datasetList = new ArrayList<Dataset>();

        for (String id : datasetIdList) {
            Dataset icatDs = ICATClientFactory.getInstance().createICATInterface().getDatasetForDatasetId(sessionId, Long.parseLong(id));
            datasetList.add(icatDs);
        }
        addPreprocessedDatasets(requestEntity, datasetList);
    }
	
	public void addDatasetsFromDatafiles(String sessionId, RequestEntity requestEntity, String datafileIds) throws Exception {
		List<String> datasetIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
        List<Dataset> datasetList = new ArrayList<Dataset>();

        for (String id : datasetIdList) {
            Dataset icatDs = ICATClientFactory.getInstance().createICATInterface().getDatasetForDatafileId(sessionId, Long.parseLong(id));
            datasetList.add(icatDs);
        }
        addPreprocessedDatasets(requestEntity, datasetList);
	}
	
	private void addPreprocessedDatasets(RequestEntity requestEntity, List<Dataset> datasets) {
		List<Ids2DatasetEntity> newDatasetList = new ArrayList<Ids2DatasetEntity>();		
		for (Dataset icatDs : datasets) {
			Ids2DatasetEntity newDataset = new Ids2DatasetEntity();
			newDataset.setLocation(icatDs.getLocation());
            newDataset.setIcatDatasetId(icatDs.getId());
            newDataset.setRequest(requestEntity);
            newDataset.setStatus(StatusInfo.SUBMITTED);         
            newDatasetList.add(newDataset);
            em.persist(newDataset);
		}		
		requestEntity.setDatasets(newDatasetList);
        em.merge(requestEntity);
        em.flush();
	}
	
	public void setDatasetStatus(Ids2DatasetEntity ds, StatusInfo status) {
		ds = em.merge(ds);
		ds.setStatus(StatusInfo.COMPLETED);
		setRequestCompletedIfEverythingDone(ds);
		em.merge(ds);
	}
	
	private void setRequestCompletedIfEverythingDone(Ids2DatasetEntity dataset) {
		RequestEntity request = dataset.getRequest();
		for (Ids2DatasetEntity ds : request.getDatasets()) {
			if (ds.getStatus() != StatusInfo.COMPLETED) {
				return;
			}
		}
		request.setStatus(StatusInfo.COMPLETED);
	}
	
	public RequestEntity getRequestByPreparedId(String preparedId) {
		Query q = em.createQuery("SELECT d FROM RequestEntity d WHERE d.preparedId = :preparedId").setParameter("preparedId", preparedId);
        return (RequestEntity) q.getSingleResult();
	}

}
