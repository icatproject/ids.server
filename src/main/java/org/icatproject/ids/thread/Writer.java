package org.icatproject.ids.thread;

import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.util.RequestHelper;

public class Writer implements Runnable {

//	private final static Logger logger = LoggerFactory.getLogger(ProcessQueue.class);
//
//	private IdsDataEntity de;
//	private RequestQueues requestQueues;
//	private RequestHelper requestHelper;
	
	public Writer(IdsDataEntity de, RequestHelper requestHelper) {
//		this.de = de;
//		this.requestQueues = RequestQueues.getInstance();
//		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
//		logger.info("starting writer");
//		StorageInterface storageInterface = StorageFactory.getInstance().createStorageInterface();
//		StatusInfo resultingStatus = storageInterface.writeToArchive(de.getIcatDataset());
//		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
//		Set<Dataset> changing = requestQueues.getChanging();
//		synchronized (deferredOpsQueue) {
//			logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
//			requestHelper.setDataEntityStatus(de, resultingStatus);
//			changing.remove(de.getIcatDataset());
//		}
	}
}
