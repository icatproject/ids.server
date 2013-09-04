package org.icatproject.ids2.ported.thread;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import org.icatproject.Dataset;
import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//copies files from the archive to the local storage (in zip), also creates an unzipped copy
public class Restorer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(ProcessQueue.class);

	private Ids2DataEntity de;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public Restorer(Ids2DataEntity de, RequestHelper requestHelper) {
		this.de = de;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting restorer");
		StorageInterface slowStorageInterface = StorageFactory.getInstance().createSlowStorageInterface();
		StorageInterface fastStorageInterface = StorageFactory.getInstance().createFastStorageInterface();
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // assuming, that everything will go OK
		try {
			if (!fastStorageInterface.datasetExists(de.getIcatDataset())) {
				InputStream is = slowStorageInterface.getDatasetInputStream(de.getIcatDataset());
				fastStorageInterface.putDataset(de.getIcatDataset(), is);
			}
		} catch (FileNotFoundException e) {
			logger.warn("Could not restore " + de.getIcatDataset() + " (file doesn't exist): " + e.getMessage());
			resultingStatus = StatusInfo.NOT_FOUND;
		} catch (Exception e) {
			logger.warn("Could not restore " + de.getIcatDataset() + " (reason uknonwn): " + e.getMessage());
			resultingStatus = StatusInfo.ERROR;
		}
		
		Map<Ids2DataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		synchronized (deferredOpsQueue) {
			logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
			requestHelper.setDataEntityStatus(de, resultingStatus);
			changing.remove(de.getIcatDataset());
		}
	}

}