package org.icatproject.ids2.ported;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;

import org.icatproject.ids.icatclient.ICATClientBase;
import org.icatproject.ids.icatclient.ICATClientFactory;
import org.icatproject.ids.icatclient.exceptions.ICATClientException;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;

@Stateless
public class RequestHelper {

	private final static Logger logger = Logger.getLogger(RequestHelper.class.getName());
	private PropertyHandler properties = PropertyHandler.getInstance();

	@PersistenceContext(unitName = "IDS-PU")
	private EntityManager em;

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
	
	public void addDatasets(RequestEntity requestEntity, String datasetIds)
    {
        List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
        ArrayList<Ids2DatasetEntity> newDatasetList = new ArrayList<Ids2DatasetEntity>();

        for (String id : datasetIdList) {
            Ids2DatasetEntity newDataset = new Ids2DatasetEntity();
            newDataset.setIcatDatasetId(Long.parseLong(id));
            newDataset.setRequest(requestEntity);
            newDataset.setStatus(StatusInfo.SUBMITTED);
//            newDataset.setDatasetName(null); // TODO: check if this field is needed, if not remove
//            newDataset = em.merge(newDataset); // new            
            newDatasetList.add(newDataset);
            em.persist(newDataset); // old
        }
        
        requestEntity.setDatasets(newDatasetList);
        em.merge(requestEntity);
        em.flush();
//        logger.info("refresh in addDatasets");
//        logger.info("addDatasets em contains " + em.contains(requestEntity.getDatasetList().get(0)));
//        em.refresh(requestEntity.getDatasetList().get(0));
//        logger.info("after refresh in addDatasets");
    }
	
	public void setDatasetStatus(Ids2DatasetEntity ds, StatusInfo status) {
		ds = em.merge(ds);
		ds.setStatus(StatusInfo.COMPLETED);
		RequestEntity requestEntity = ds.getRequest();
		requestEntity.setStatus(StatusInfo.COMPLETED);
		em.merge(ds);
	}
	
	public RequestEntity getRequestByPreparedId(String preparedId) {
		Query q = em.createQuery("SELECT d FROM RequestEntity d WHERE d.preparedId = :preparedId").setParameter("preparedId", preparedId);
        return (RequestEntity) q.getSingleResult();
	}

}
