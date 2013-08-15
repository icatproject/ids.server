package org.icatproject.ids2.ported.thread;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.icatproject.ids2.ported.entity.RequestEntity;

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
		StorageInterface storageInterface = StorageFactory.getInstance().createStorageInterface();
		StatusInfo resultingStatus = storageInterface.restoreFromArchive(de.getDatasets());
		Map<Ids2DataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Ids2DataEntity> changing = requestQueues.getChanging();
		synchronized (deferredOpsQueue) {
			logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
			requestHelper.setDataEntityStatus(de, resultingStatus);
			changing.remove(de);
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