package org.icatproject.ids2.ported.thread;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.util.ZipHelper;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//copies files from the archive to the local storage (in zip), also creates an unzipped copy
public class Preparer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(ProcessQueue.class);

	private Ids2DataEntity de;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public Preparer(Ids2DataEntity de, RequestHelper requestHelper) {
		this.de = de;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting preparer");
		Map<Ids2DataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		
		// if one of the previous DataEntities of the Request failed, there's no point continuing with this one
		if (de.getRequest().getStatus() == StatusInfo.INCOMPLETE) {
			synchronized (deferredOpsQueue) {
				requestHelper.setDataEntityStatus(de, StatusInfo.INCOMPLETE);
			}
		}		
		// if this is the first DE of the Request being processed, set the Request status to RETRIVING
		if (de.getRequest().getStatus() == StatusInfo.SUBMITTED) {
			synchronized (deferredOpsQueue) {
				requestHelper.setRequestStatus(de.getRequest(), StatusInfo.RETRIVING);
			}
		}
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // let's assume all files are available on fast storage
		for (Datafile df : de.getIcatDatafiles()) {
			File file = new File(PropertyHandler.getInstance().getStorageDir(), df.getLocation());
			if (!file.exists()) {
				resultingStatus = StatusInfo.INCOMPLETE;
				break;
			}
		}

		if (resultingStatus.equals(StatusInfo.INCOMPLETE)) {
			StorageInterface storageInterface = StorageFactory.getInstance().createStorageInterface();
			resultingStatus = storageInterface.restoreFromArchive(de.getIcatDataset());
		}
		
		logger.info("dupa1");
		synchronized (deferredOpsQueue) {
			logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
			requestHelper.setDataEntityStatus(de, resultingStatus);
			changing.remove(de.getIcatDataset());
		}
		logger.info("dupa2 " + de.getRequest().getStatus());
		// if it's the last DataEntity of the Request and all of them were successful
		if (de.getRequest().getStatus() == StatusInfo.COMPLETED) {
			File zipFile = new File(PropertyHandler.getInstance().getStoragePreparedDir(),
					de.getRequest().getPreparedId() + ".zip");
			ZipHelper.compressFileList(zipFile, de.getRequest());
		}
	}
	
//	public void writeFileToOutputStream(RequestEntity requestEntity, OutputStream output, Long offset) throws IOException
//    {
//        File zipFile = new File(properties.getStorageZipDir() + File.separator + downloadRequestEntity.getPreparedId() + ".zip");
//
//        BufferedInputStream bis = null;
//        BufferedOutputStream bos = null;
//        try {
//            int bytesRead = 0;
//            byte[] buffer = new byte[32 * 1024];
//            bis = new BufferedInputStream(new FileInputStream(zipFile));
//            bos = new BufferedOutputStream(output);
//            
//            // apply offset to stream
//            if (offset > 0) {
//                bis.skip(offset);
//            }
//            
//            // write bytes to output stream
//            while ((bytesRead = bis.read(buffer)) > 0) {
//                bos.write(buffer, 0, bytesRead);
//            }
//        } finally {
//            if (bis != null) {
//                bis.close();
//            }
//            if (bos != null) {
//                bos.close();
//            }
//        }
//    }

}