package org.icatproject.ids2.ported.thread;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//deletes files in the local storage (they'll stay in the archive)
public class Archiver implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(ProcessQueue.class);

	private IdsDataEntity de;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public Archiver(IdsDataEntity de, RequestHelper requestHelper) {
		this.de = de;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting archiver");
		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // assuming that everything will go OK
		Dataset ds = de.getIcatDataset();
		
		try {
			StorageInterface slowStorageInterface = StorageFactory.getInstance().createSlowStorageInterface();
			StorageInterface fastStorageInterface = StorageFactory.getInstance().createFastStorageInterface();
			if (fastStorageInterface.datasetExists(ds)) {
				InputStream is = fastStorageInterface.getDatasetInputStream(ds);
				slowStorageInterface.putDataset(ds, is);
				fastStorageInterface.deleteDataset(ds);
			}
			logger.info("Archive of  " + ds.getLocation() + " succesful");
		} catch (Exception e) {
			logger.error("Archive of " + ds.getLocation() + " failed due to " + e.getMessage());
			resultingStatus = StatusInfo.INCOMPLETE;
		} finally {
			synchronized (deferredOpsQueue) {
				logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
				requestHelper.setDataEntityStatus(de, resultingStatus);
				changing.remove(de.getIcatDataset());
			}
		}
	}
}
