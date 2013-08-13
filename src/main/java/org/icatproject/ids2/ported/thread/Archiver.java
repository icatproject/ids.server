package org.icatproject.ids2.ported.thread;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;

//deletes files in the local storage (they'll stay in the archive)
public class Archiver implements Runnable {
	private final static Logger logger = Logger.getLogger(ProcessQueue.class.getName());

	private Ids2DataEntity de;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public Archiver(Ids2DataEntity de, RequestHelper requestHelper) {
		this.de = de;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting archiver");
		Map<Ids2DataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Ids2DataEntity> changing = requestQueues.getChanging();
		String storageDir = PropertyHandler.getInstance().getStorageDir();
		String storageZipDir = PropertyHandler.getInstance().getStorageZipDir();
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // assuming that everything will go OK
		
		try {
			for (Dataset ds : de.getDatasets()) {
				try {
					File dir = new File(storageDir, ds.getLocation());
					File zipdir = new File(storageZipDir, ds.getLocation());
					FileUtils.deleteDirectory(dir);
					FileUtils.deleteDirectory(zipdir);
					logger.info("Archive of  " + ds.getLocation() + " succesful");
				} catch (Exception e) {
					logger.severe("Archive of " + ds.getLocation() + " failed");
					resultingStatus = StatusInfo.INCOMPLETE;
				}
			}
		} finally {
			if (resultingStatus == StatusInfo.COMPLETED) {
				logger.info("Archive of " + de + " completed successfully");
			}
			else {
				logger.warning("Archive of " + de + " completed with errors");
			}
			synchronized (deferredOpsQueue) {
				logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
				requestHelper.setDataEntityStatus(de, resultingStatus);
				changing.remove(de);
			}
		}
	}
}
