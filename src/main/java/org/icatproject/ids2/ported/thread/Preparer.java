package org.icatproject.ids2.ported.thread;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.icatproject.Datafile;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.util.ZipHelper;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;

//copies files from the archive to the local storage (in zip), also creates an unzipped copy
public class Preparer implements Runnable {

	private final static Logger logger = Logger.getLogger(ProcessQueue.class.getName());

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
		Set<Ids2DataEntity> changing = requestQueues.getChanging();
		// assuming restoration of the files is not needed, all files should be available on fast storage
//		StorageInterface storageInterface = StorageFactory.getInstance().createStorageInterface();
//		StatusInfo resultingStatus = storageInterface.restoreFromArchive(de.getDatasets());
		
		// if one of the previous DataEntities of the Request failed, there's no point continuing with this one
		if (de.getRequest().getStatus() == StatusInfo.INCOMPLETE) {
			requestHelper.setDataEntityStatus(de, StatusInfo.INCOMPLETE);
		}		
		// if this is the first DE of the Request being processed, set the Request status to RETRIVING
		if (de.getRequest().getStatus() == StatusInfo.SUBMITTED) {
			requestHelper.setRequestStatus(de.getRequest(), StatusInfo.RETRIVING);
		}
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // let's assume everything will go OK
		for (Datafile df : de.getIcatDatafiles()) {
			File file = new File(PropertyHandler.getInstance().getStorageDir(), df.getLocation());
			if (!file.exists()) {
				resultingStatus = StatusInfo.INCOMPLETE;
				break;
			}
		}
		logger.info("dupa1");
		synchronized (deferredOpsQueue) {
			logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
			requestHelper.setDataEntityStatus(de, resultingStatus);
			changing.remove(de);
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