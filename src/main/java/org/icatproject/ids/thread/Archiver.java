package org.icatproject.ids.thread;

import java.util.Map;
import java.util.Set;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.plugin.StorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.RequestHelper;
import org.icatproject.ids.util.RequestQueues;
import org.icatproject.ids.util.RequestedState;
import org.icatproject.ids.util.StatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Removes datasets from the fast storage (doesn't write them to slow storage)
 */
public class Archiver implements Runnable {
	private final static Logger logger = LoggerFactory.getLogger(Archiver.class);

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
		logger.info("starting Archiver");
		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		StorageInterface fastStorageInterface = PropertyHandler.getInstance()
				.getMainStorage();

		StatusInfo resultingStatus = StatusInfo.COMPLETED; // assuming that everything will go OK
		Dataset ds = de.getIcatDataset();
		try {
			fastStorageInterface.deleteDataset(ds.getLocation());
			for (Datafile df : ds.getDatafiles()) {
				fastStorageInterface.deleteDatafile(df.getLocation());
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
