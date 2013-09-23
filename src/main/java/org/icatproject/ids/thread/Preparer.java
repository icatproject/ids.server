package org.icatproject.ids.thread;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.storage.StorageFactory;
import org.icatproject.ids.storage.StorageInterface;
import org.icatproject.ids.util.RequestHelper;
import org.icatproject.ids.util.RequestQueues;
import org.icatproject.ids.util.RequestedState;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.util.ZipHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Prepares zips for users to download using /getData
 */
public class Preparer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(ProcessQueue.class);

	private IdsDataEntity de;
	private RequestQueues requestQueues;
	private RequestHelper requestHelper;

	public Preparer(IdsDataEntity de, RequestHelper requestHelper) {
		this.de = de;
		this.requestQueues = RequestQueues.getInstance();
		this.requestHelper = requestHelper;
	}
	
	@Override
	public void run() {
		logger.info("starting preparer");
		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		StorageInterface fastStorageInterface = StorageFactory.getInstance().createFastStorageInterface();
		StorageInterface slowStorageInterface = StorageFactory.getInstance().createSlowStorageInterface();
		
		// if one of the previous DataEntities of the Request failed, there's no point continuing with this one
//		if (de.getRequest().getStatus() == StatusInfo.INCOMPLETE) {
//			synchronized (deferredOpsQueue) {
//				requestHelper.setDataEntityStatus(de, StatusInfo.INCOMPLETE);
//			}
//		}		
		// if this is the first DE of the Request being processed, set the Request status to RETRIVING
		if (de.getRequest().getStatus() == StatusInfo.SUBMITTED) {
			synchronized (deferredOpsQueue) {
				requestHelper.setRequestStatus(de.getRequest(), StatusInfo.RETRIVING);
			}
		}
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // let's assume that everything will go OK		
		// restore the dataset if needed
		InputStream slowIS = null;
		ZipInputStream fastIS = null;
		try {
			if (!fastStorageInterface.datasetExists(de.getIcatDataset())) {
				if (slowStorageInterface == null) {
					logger.error("Preparer can't perform because there's no slow storage");
					resultingStatus = StatusInfo.ERROR;
				} else {
					slowIS = slowStorageInterface.getDataset(de.getIcatDataset());
					fastStorageInterface.putDataset(de.getIcatDataset(), slowIS);
					fastIS = new ZipInputStream(fastStorageInterface.getDataset(de.getIcatDataset()));
					ZipEntry entry;
					while ((entry = fastIS.getNextEntry()) != null) {
						if (entry.isDirectory()) {
							continue;
						}
						String datafileLocation = new File(de.getIcatDataset().getLocation(), entry.getName()).getPath();
						fastStorageInterface.putDatafile(datafileLocation, fastIS);
					}
				}
			}
		} catch (FileNotFoundException e) {
			logger.warn("Could not restore " + de.getIcatDataset() + " (file doesn't exist): " + e.getMessage());
			resultingStatus = StatusInfo.NOT_FOUND;
		} catch (Exception e) {
			logger.warn("Could not restore " + de.getIcatDataset() + " (reason uknonwn): " + e.getMessage());
			resultingStatus = StatusInfo.ERROR;
		} finally {
			if (slowIS != null) {
				try {
					slowIS.close();
				} catch (IOException e) {
					logger.warn("Couldn't close an input stream from the slow storage");
				}
			}
			if (fastIS != null) {
				try {
					fastIS.close();
				} catch (IOException e) {
					logger.warn("Couldn't close an input stream from the fast storage");
				}
			}
		}
		
		synchronized (deferredOpsQueue) {
			logger.info(String.format("Changing status of %s to %s", de, resultingStatus));
			requestHelper.setDataEntityStatus(de, resultingStatus);
			changing.remove(de.getIcatDataset());
		}
		
		// if it's the last DataEntity of the Request and all of them were successful
		if (de.getRequest().getStatus() == StatusInfo.COMPLETED) {
			try {
				InputStream is = ZipHelper.prepareZipForUserRequest(de.getRequest(), fastStorageInterface);
				fastStorageInterface.putPreparedZip(de.getRequest().getPreparedId() + ".zip", is);
			} catch (Exception e) {
				logger.warn(String.format("Could not prepare the zip. Reason: " + e.getMessage()));
				synchronized (deferredOpsQueue) {
					logger.info(String.format("Changing status of %s to %s", de, StatusInfo.ERROR));
					requestHelper.setDataEntityStatus(de, resultingStatus);
					changing.remove(de.getIcatDataset());
				}
			}
		}
	}

}