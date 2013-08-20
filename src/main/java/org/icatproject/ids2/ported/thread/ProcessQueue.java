package org.icatproject.ids2.ported.thread;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.icatproject.Dataset;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;
import org.icatproject.ids2.ported.entity.Ids2DataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessQueue extends TimerTask {

	private final static Logger logger = LoggerFactory.getLogger(ProcessQueue.class);

	private RequestQueues requestQueues;
	private Timer timer;
	private RequestHelper requestHelper;

	public ProcessQueue(Timer timer, RequestHelper requestHelper) {
		this.requestQueues = RequestQueues.getInstance();
		this.timer = timer;
		this.requestHelper = requestHelper;
	}

	@Override
	public void run() {
		Map<Ids2DataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		Map<Dataset, Long> writeTimes = requestQueues.getWriteTimes();

		try {
			synchronized (deferredOpsQueue) {
				final long now = System.currentTimeMillis();
				final Iterator<Entry<Ids2DataEntity, RequestedState>> it = deferredOpsQueue.entrySet().iterator();
				while (it.hasNext()) {
					final Entry<Ids2DataEntity, RequestedState> opEntry = it.next();
					final Ids2DataEntity de = opEntry.getKey();
					logger.info("Processing " + de);
					if (!changing.contains(de)) {
						// Dataset icatDs = (Dataset)
						// this.icatClient.get(sessionId, "Dataset",
						// ds.getDatasetId());
						// ds.setLocation(icatDs.getLocation());
						final RequestedState state = opEntry.getValue();
						logger.info("Will process " + de + " with " + state);
						if (state == RequestedState.WRITE_REQUESTED) {
							if (now > writeTimes.get(de.getIcatDataset())) {
								writeTimes.remove(de.getIcatDataset());
								changing.add(de.getIcatDataset());
								it.remove();
								// final Thread w = new Thread(new
								// Writer(location));
								// w.start();
							}
						} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
							if (now > writeTimes.get(de.getIcatDataset())) {
								writeTimes.remove(de.getIcatDataset());
								changing.add(de.getIcatDataset());
								it.remove();
								// final Thread w = new Thread(new
								// WriteThenArchiver(location));
								// w.start();
							}
						} else if (state == RequestedState.ARCHIVE_REQUESTED) {
							changing.add(de.getIcatDataset());
							it.remove();
							final Thread w = new Thread(new Archiver(de, requestHelper));
							w.start();
						} else if (state == RequestedState.RESTORE_REQUESTED) {
							changing.add(de.getIcatDataset());
							it.remove();
							final Thread w = new Thread(new Restorer(de, requestHelper));
							w.start();
						} else if (state == RequestedState.PREPARE_REQUESTED) {
							changing.add(de.getIcatDataset());
							it.remove();
							final Thread w = new Thread(new Preparer(de, requestHelper));
							w.start();
						}
					}
				}
			}
		} finally {
			// logger.info("Starting new Timer from ProcessQueue");
			timer.schedule(new ProcessQueue(timer, requestHelper), PropertyHandler.getInstance()
					.getProcessQueueIntervalSeconds() * 1000L);
		}

	}

}