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
import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.icatproject.ids2.ported.entity.Ids2DatafileEntity;
import org.icatproject.ids2.ported.entity.Ids2DatasetEntity;
import org.icatproject.ids2.ported.entity.RequestEntity;

@Stateless
public class RequestHelper {

	private final static Logger logger = Logger.getLogger(RequestHelper.class.getName());
	private PropertyHandler properties = PropertyHandler.getInstance();
	private ICATClientBase icatClient;

	@PersistenceContext(unitName = "IDS-PU")
	private EntityManager em;
	
	@PostConstruct
	public void postConstruct() throws MalformedURLException, ICATClientException {
		icatClient = ICATClientFactory.getInstance().createICATInterface();
	}

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
	        List<Ids2DatasetEntity> newDatasetList = new ArrayList<Ids2DatasetEntity>();

	        for (String id : datasetIdList) {
	            Ids2DatasetEntity newDataset = new Ids2DatasetEntity();
	            newDataset.setIcatDatasetId(Long.parseLong(id));
	            newDataset.setIcatDataset(icatClient.getDatasetForDatasetId(sessionId, Long.parseLong(id)));
	            newDataset.setRequest(requestEntity);
	            newDataset.setStatus(StatusInfo.SUBMITTED);         
	            newDatasetList.add(newDataset);
	            em.persist(newDataset);
	        }
	        
	        requestEntity.setDatasets(newDatasetList);
	        em.merge(requestEntity);
	        em.flush();
    }
	
	public void addDatafiles(String sessionId, RequestEntity requestEntity, String datafileIds) throws Exception {
		List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
		List<Ids2DatafileEntity> newDatafileList = new ArrayList<Ids2DatafileEntity>();
		
		for (String id : datafileIdList) {
            Ids2DatafileEntity newDatafile = new Ids2DatafileEntity();
            newDatafile.setIcatDatafileId(Long.parseLong(id));
            newDatafile.setIcatDatafile(icatClient.getDatafileWithDatasetForDatafileId(sessionId, Long.parseLong(id)));
            newDatafile.setRequest(requestEntity);
            newDatafile.setStatus(StatusInfo.SUBMITTED);         
            newDatafileList.add(newDatafile);
            em.persist(newDatafile);
        }
        
        requestEntity.setDatafiles(newDatafileList);
        em.merge(requestEntity);
        em.flush();
	}
	
//	public void addDatasetsFromDatafiles(String sessionId, RequestEntity requestEntity, String datafileIds) throws Exception {
//		List<String> datasetIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));
//        List<Dataset> datasetList = new ArrayList<Dataset>();
//
//        for (String id : datasetIdList) {
//            Dataset icatDs = ICATClientFactory.getInstance().createICATInterface().getDatasetForDatafileId(sessionId, Long.parseLong(id));
//            datasetList.add(icatDs);
//        }
//        addPreprocessedDatasets(requestEntity, datasetList);
//	}
//	
//	private void addPreprocessedDatasets(RequestEntity requestEntity, List<Dataset> datasets) {
//		List<Ids2DatasetEntity> newDatasets = new ArrayList<Ids2DatasetEntity>();		
//		for (Dataset icatDs : datasets) {
//			Ids2DatasetEntity newDataset = new Ids2DatasetEntity();
//			newDataset.setLocation(icatDs.getLocation());
//            newDataset.setIcatDatasetId(icatDs.getId());
//            newDataset.setRequest(requestEntity);
//            newDataset.setStatus(StatusInfo.SUBMITTED);         
//            newDatasets.add(newDataset);
//            em.persist(newDataset);
//		}		
//		requestEntity.setDatasets(newDatasets);
//        em.merge(requestEntity);
//        em.flush();
//	}
//	
	public void setDatasetStatus(Ids2DataEntity ds, StatusInfo status) {
		ds = em.merge(ds);
		ds.setStatus(StatusInfo.COMPLETED);
		setRequestCompletedIfEverythingDone(ds);
		em.merge(ds);
	}
	
	private void setRequestCompletedIfEverythingDone(Ids2DataEntity dataset) {
		RequestEntity request = dataset.getRequest();
		logger.info("Will check status of " + request.getDatasets().size() + " datasets");
		for (Ids2DatasetEntity ds : request.getDatasets()) {
			logger.info("Retrieval status of " + ds + " is " + ds.getStatus());
			if (ds.getStatus() != StatusInfo.COMPLETED) {
				logger.info("Retrieval of " + ds + " still not completed");
				return;
			}
		}
		logger.info("all tasks completed");
		request.setStatus(StatusInfo.COMPLETED);
	}
	
	public RequestEntity getRequestByPreparedId(String preparedId) {
		Query q = em.createQuery("SELECT d FROM RequestEntity d WHERE d.preparedId = :preparedId").setParameter("preparedId", preparedId);
        return (RequestEntity) q.getSingleResult();
	}

}
