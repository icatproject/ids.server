package org.icatproject.ids.thread;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.RequestHelper;
import org.icatproject.ids.util.RequestQueues;
import org.icatproject.ids.util.RequestedState;
import org.icatproject.ids.util.StatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Copies datasets across storages (fast to slow) and subsequently
 * removes the files on the fast storage
 */
public class WriteThenArchiver implements Runnable {
	
	private final static Logger logger = LoggerFactory.getLogger(WriteThenArchiver.class);

	private IdsDataEntity de;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public WriteThenArchiver(IdsDataEntity de, RequestHelper requestHelper) {
		this.de = de;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting WriteThenArchiver");
		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		StorageInterface slowStorageInterface = StorageFactory.getInstance().createSlowStorageInterface();
		StorageInterface fastStorageInterface = StorageFactory.getInstance().createFastStorageInterface();
		
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // assuming that everything will go OK
		Dataset ds = de.getIcatDataset();		
		try {
			if (slowStorageInterface == null) {
				logger.error("WriteThenArchiver can't perform because there's no slow storage");
				resultingStatus = StatusInfo.ERROR;
				return;
			}
			if (fastStorageInterface.datasetExists(ds)) {
				InputStream is = fastStorageInterface.getDataset(ds);
				slowStorageInterface.putDataset(ds, is);
				fastStorageInterface.deleteDataset(ds);
				for (Datafile df : ds.getDatafiles()) {
					fastStorageInterface.deleteDatafile(df);
				}
			}
			logger.info("WriteThenArchive of  " + ds.getLocation() + " succesful");
		} catch (Exception e) {
			logger.error("WriteThenArchive of " + ds.getLocation() + " failed due to " + e.getMessage());
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
