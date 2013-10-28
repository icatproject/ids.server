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
import org.icatproject.ids.plugin.StorageInterface;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.RequestHelper;
import org.icatproject.ids.util.RequestQueues;
import org.icatproject.ids.util.RequestQueues.RequestedState;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.util.ZipHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Prepares zips for users to download using /getData
 */
public class Preparer implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(Preparer.class);

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
		StorageInterface fastStorageInterface = PropertyHandler.getInstance().getMainStorage();
		StorageInterface slowStorageInterface = PropertyHandler.getInstance().getArchiveStorage();

		if (de.getRequest().getStatus() == StatusInfo.SUBMITTED) {
			synchronized (deferredOpsQueue) {
				requestHelper.setRequestStatus(de.getRequest(), StatusInfo.RETRIEVING);
			}
		}
		StatusInfo resultingStatus = StatusInfo.COMPLETED; // let's assume that everything will go
															// OK
		// restore the dataset if needed
		InputStream slowIS = null;
		ZipInputStream fastIS = null;
		try {
			if (!fastStorageInterface.datasetExists(de.getIcatDataset().getLocation())) {
				if (slowStorageInterface == null) {
					logger.error("Preparer can't perform because there's no slow storage");
					resultingStatus = StatusInfo.ERROR;
				} else {
					slowIS = slowStorageInterface.getDataset(de.getIcatDataset().getLocation());
					fastStorageInterface.putDataset(de.getIcatDataset().getLocation(), slowIS);
					slowIS.close();
					fastIS = new ZipInputStream(fastStorageInterface.getDataset(de.getIcatDataset()
							.getLocation()));
					ZipEntry entry;
					while ((entry = fastIS.getNextEntry()) != null) {
						if (entry.isDirectory()) {
							continue;
						}
						String datafileLocation = new File(de.getIcatDataset().getLocation(),
								entry.getName()).getPath();
						fastStorageInterface.putDatafile(datafileLocation, fastIS);
					}
				}
			}
		} catch (FileNotFoundException e) {
			logger.warn("Could not restore " + de.getIcatDataset() + " (file doesn't exist): "
					+ e.getMessage());
			resultingStatus = StatusInfo.NOT_FOUND;
		} catch (Exception e) {
			logger.warn("Could not restore " + de.getIcatDataset() + " (reason uknonwn): "
					+ e.getMessage());
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
				InputStream is = ZipHelper.prepareZipForUserRequest(de.getRequest(),
						fastStorageInterface);
				fastStorageInterface.putPreparedZip(de.getRequest().getPreparedId() + ".zip", is);
				is.close();
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