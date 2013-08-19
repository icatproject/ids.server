package org.icatproject.ids.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
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

import org.icatproject.ids.entity.DatafileEntity;
import org.icatproject.ids.entity.DatasetEntity;
import org.icatproject.ids.entity.DownloadRequestEntity;
import org.icatproject.ids.icatclient.ICATClientBase;
import org.icatproject.ids.icatclient.ICATClientFactory;
import org.icatproject.ids.icatclient.exceptions.ICATClientException;
import org.icatproject.ids.icatclient.exceptions.ICATInsufficientPrivilegesException;
import org.icatproject.ids.icatclient.exceptions.ICATNoSuchObjectException;


/**
 * This class provides all the methods that interact with the persistent storage as well as some
 * methods to interact with ICAT and the storage.
 */
@Stateless
public class DownloadRequestHelper
{
	private final static Logger logger = Logger.getLogger(DownloadRequestHelper.class.getName());
	
    @PersistenceContext(unitName = "IDS-PU")
    private EntityManager em;

    private PropertyHandler properties = PropertyHandler.getInstance();

    
    public DownloadRequestEntity createDownloadRequest(String sessionId, String compress, String zip) 
        throws ICATClientException, MalformedURLException
    {
        ICATClientBase client = ICATClientFactory.getInstance().createICATInterface();
        Calendar expireDate = Calendar.getInstance();
        expireDate.add(Calendar.DATE, properties.getNumberOfDaysToExpire());
        
        String username = client.getUserId(sessionId);   
        
        DownloadRequestEntity downloadRequestEntity = new DownloadRequestEntity();
        downloadRequestEntity.setSessionId(sessionId);
        downloadRequestEntity.setUserid(username);
        downloadRequestEntity.setPreparedId(UUID.randomUUID().toString());
        downloadRequestEntity.setStatus(StatusInfo.SUBMITTED.name());
        downloadRequestEntity.setCompress(Boolean.parseBoolean(compress));
        downloadRequestEntity.setSubmittedTime(new Date());
        downloadRequestEntity.setExpireTime(expireDate.getTime()); 
 
        em.persist(downloadRequestEntity);
        em.flush();

        return downloadRequestEntity;
    }


    public void addDatasets(DownloadRequestEntity downloadRequestEntity, String datasetIds)
    {
        List<String> datasetIdList = Arrays.asList(datasetIds.split("\\s*,\\s*"));
        ArrayList<DatasetEntity> newDatasetList = new ArrayList<DatasetEntity>();

        for (String id : datasetIdList) {
            DatasetEntity newDataset = new DatasetEntity();
            newDataset.setDatasetid(Long.parseLong(id));
            newDataset.setDownloadRequestId(downloadRequestEntity);
            newDataset.setStatus(StatusInfo.SUBMITTED.name());
            newDataset.setDatasetName(null); // TODO: check if this field is needed, if not remove
            newDatasetList.add(newDataset);
            em.persist(newDataset);
        }
        
        downloadRequestEntity.setDatasetList(newDatasetList);
        em.merge(downloadRequestEntity);
        em.flush();
    }


    public void addDatafiles(DownloadRequestEntity downloadRequestEntity, String datafileIds)
    {
        List<String> datafileIdList = Arrays.asList(datafileIds.split("\\s*,\\s*"));;
        ArrayList<DatafileEntity> newDatafileList = new ArrayList<DatafileEntity>();
            
        for (String id : datafileIdList) {
            DatafileEntity newDatafile = new DatafileEntity();
            newDatafile.setDatafileId(Long.parseLong(id));
            newDatafile.setDownloadRequestId(downloadRequestEntity);
            newDatafile.setStatus(StatusInfo.SUBMITTED.name());
            newDatafile.setName(null); // TODO: check if this field is needed, if not remove
            newDatafileList.add(newDatafile);
            em.persist(newDatafile);
        }
        
        downloadRequestEntity.setDatafileList(newDatafileList);
        em.merge(downloadRequestEntity);
        em.flush();
    }
 

    /*
     * This method gets the download request information of datasets and datafiles
     * and extracts them from the storage source
     */
    public void processDataRetrievalRequest(DownloadRequestEntity downloadRequestEntity)
    {     
//        StorageInterface storage = StorageFactory.getInstance().createStorageInterface(em, downloadRequestEntity.getPreparedId());
//    
//        //System.out.println("Number of Datafiles:" + downloadRequestEntity.getDatafileList().size());      // TODO: remove System.out
//        em.refresh(downloadRequestEntity); // gets changes to the entity from the database
//        
//        // loop through the DownloadRequestEntity Datafiles and Datasets to download the files
//        HashSet<String> fileSet = storage.copyDatafiles(downloadRequestEntity.getDatafileList());
//        
//        // check if there were any problems retrieving the datafiles
//        for (DatafileEntity datafileEntity : downloadRequestEntity.getDatafileList()) {
//        	// logger.severe(datafileEntity.getName() + " status: " + datafileEntity.getStatus()); // TODO remove
//            if (datafileEntity.getStatus().equals(StatusInfo.ERROR.name())) {
//                downloadRequestEntity.setStatus(StatusInfo.ERROR.name());
//                // If there was a problem retrieving a file, check that it still exists in ICAT.
//                // If it no longer exists in ICAT set status as INCOMPLETE
//                try {
//                    ICATClientBase client = ICATClientFactory.getInstance().createICATInterface();
//                    ArrayList<Long> datafileIds = new ArrayList<Long>();
//                    datafileIds.add(datafileEntity.getDatafileId());
//                    client.getDatafilePaths(downloadRequestEntity.getSessionId(), datafileIds);
//                } catch (ICATNoSuchObjectException e) {
//                    downloadRequestEntity.setStatus(StatusInfo.INCOMPLETE.name());
//                } catch (Exception e) {
//                    // do nothing as status already set as error
//                }
//                break;
//            }
//        }
//        
//        // if there were not any problems retriving the files, change the status of the request to COMPLETE
//        if (!downloadRequestEntity.getStatus().equals(StatusInfo.ERROR.name())) {
//            downloadRequestEntity.setStatus(StatusInfo.COMPLETED.name());
//            File zipFile = new File(properties.getStorageZipDir() + File.separator + downloadRequestEntity.getPreparedId() + ".zip");
//            ZipHelper.compressFileList(zipFile, fileSet, properties.getStorageDir(), downloadRequestEntity.getCompress());
//        }
//    
//        em.merge(downloadRequestEntity);
    }


    /*
     * This method gets the dataset and datafiles information from icat and
     * adds them to the database
     */
    public void processInfoRetrievalRequest(DownloadRequestEntity downloadRequestEntity) throws MalformedURLException 
    {
        try {
            getDatafileInformation(downloadRequestEntity);
            getDatafilesInDatasetsInformation(downloadRequestEntity);
            downloadRequestEntity.setStatus(StatusInfo.INFO_RETRIVED.name());
        } catch (ICATNoSuchObjectException e) {
            downloadRequestEntity.setStatus(StatusInfo.NOT_FOUND.name());
        } catch (ICATInsufficientPrivilegesException e) {
            downloadRequestEntity.setStatus(StatusInfo.DENIED.name());
        } catch (ICATClientException e) {
            downloadRequestEntity.setStatus(StatusInfo.ERROR.name());
        }
        
        em.merge(downloadRequestEntity);
        em.flush();
    }
  
    
    private void getDatafileInformation(DownloadRequestEntity downloadRequestEntity) 
        throws ICATClientException, MalformedURLException
    {
        //Collect the datafile id's in 100's from icat and populate the Database
        int count = 0;
        int maxIndex = 100;
        List<DatafileEntity> datafileList = downloadRequestEntity.getDatafileList();
        ArrayList<Long> datafileIds = new ArrayList<Long>();
        ICATClientBase client = ICATClientFactory.getInstance().createICATInterface();
        
        while (count < datafileList.size()) {
            datafileIds.clear();
            try {
                for (int idx = 0; idx < maxIndex; idx++) {
                    datafileIds.add(datafileList.get(count + idx).getDatafileId());
                }
            } catch (IndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            
            ArrayList<String> filePathList = client.getDatafilePaths(downloadRequestEntity.getSessionId(), datafileIds);
                       
            for (String filePath : filePathList) {
                datafileList.get(count).setName(filePath);
                datafileList.get(count).setStatus(StatusInfo.INFO_RETRIVED.name());
                count++;
            }
        }
    }


    private void getDatafilesInDatasetsInformation(DownloadRequestEntity downloadRequestEntity) throws ICATClientException, MalformedURLException
    {
        List<DatasetEntity> datasetList = downloadRequestEntity.getDatasetList();
        ICATClientBase client = ICATClientFactory.getInstance().createICATInterface();
        
        for (DatasetEntity datasetEntity : datasetList) {
            //get the list of datafiles in the dataset and then process them
            List<DatafileEntity> datafileList = client.getDatafilesInDataset(downloadRequestEntity.getSessionId(), datasetEntity.getDatasetId());
            
            //persist the data file list
            for (DatafileEntity datafileEntity : datafileList) {
                datafileEntity.setDatasetId(datasetEntity.getId());
                datafileEntity.setDownloadRequestId(downloadRequestEntity);
                datafileEntity.setStatus(StatusInfo.INFO_RETRIVED.name());
                em.persist(datafileEntity);
            }
            em.flush();
            
            datasetEntity.setStatus(StatusInfo.INFO_RETRIVED.name());
            em.merge(datasetEntity);
        }
    }


    public void writeFileToOutputStream(DownloadRequestEntity downloadRequestEntity, OutputStream output, Long offset) throws IOException
    {
        File zipFile = new File(properties.getStorageZipDir() + File.separator + downloadRequestEntity.getPreparedId() + ".zip");

        BufferedInputStream bis = null;
        BufferedOutputStream bos = null;
        try {
            int bytesRead = 0;
            byte[] buffer = new byte[32 * 1024];
            bis = new BufferedInputStream(new FileInputStream(zipFile));
            bos = new BufferedOutputStream(output);
            
            // apply offset to stream
            if (offset > 0) {
                bis.skip(offset);
            }
            
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


    public DownloadRequestEntity getDownloadRequestById(Long id)
    {
        Query q = em.createQuery("SELECT d FROM DownloadRequestEntity d WHERE d.id = :id").setParameter("id", id);
        return (DownloadRequestEntity) q.getSingleResult();
    }
    
    
    public DownloadRequestEntity getDownloadRequestByPreparedId(String preparedId)
    {
        Query q = em.createQuery("SELECT d FROM DownloadRequestEntity d WHERE d.preparedId = :preparedId").setParameter("preparedId", preparedId);
        return (DownloadRequestEntity) q.getSingleResult();
    }


    @SuppressWarnings("unchecked")
    public List<DownloadRequestEntity> getDownloadRequestFromDatasetId(Long datasetId)
    {
        Query q = em.createQuery("SELECT ds.downloadRequestId FROM DatasetEntity ds WHERE ds.datasetId = :datasetId").setParameter("datasetId", datasetId);
        return (List<DownloadRequestEntity>) q.getResultList();
    }


    @SuppressWarnings("unchecked")
    public List<DownloadRequestEntity> getDownloadRequestFromDatafileId(Long datafileId)
    {
        Query q = em.createQuery("SELECT df.downloadRequestId FROM DatafileEntity df WHERE df.datafileId = :datafileId").setParameter("datafileId", datafileId);
        return (List<DownloadRequestEntity>) q.getResultList();
    }
    
   
    public void deleteDownloadRequest(DownloadRequestEntity downloadRequestEntity)
    {
        File file = new File(properties.getStorageZipDir() + File.separator + downloadRequestEntity.getPreparedId() + ".zip");
        file.delete();
        
        em.remove(downloadRequestEntity);
        em.flush();
    }
    
    
    @SuppressWarnings("unchecked")
    public void removeExpiredDownloadRequests()
    {
        Query q = em.createQuery("SELECT d FROM DownloadRequestEntity d WHERE d.expireTime < :expireTime").setParameter("expireTime", new Date());
        for(DownloadRequestEntity downloadRequestEntity : (List<DownloadRequestEntity>) q.getResultList()) {
            deleteDownloadRequest(downloadRequestEntity);
        }
        em.flush();
    }
}
