package org.icatproject.ids2.ported.thread;

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
import org.icatproject.ids2.ported.entity.Ids2DatasetEntity;

//copies files from the archive to the local storage (in zip), also creates an unzipped copy
public class Restorer implements Runnable {

	private final static Logger logger = Logger.getLogger(ProcessQueue.class.getName());

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

}