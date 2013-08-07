package org.icatproject.ids2.ported.thread;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids2.ported.Ids2DatasetEntity;
import org.icatproject.ids2.ported.RequestHelper;
import org.icatproject.ids2.ported.RequestQueues;
import org.icatproject.ids2.ported.RequestedState;

public class ProcessQueue extends TimerTask {

	private final static Logger logger = Logger.getLogger(ProcessQueue.class.getName());

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
		Map<Ids2DatasetEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Ids2DatasetEntity> changing = requestQueues.getChanging();
		Map<Ids2DatasetEntity, Long> writeTimes = requestQueues.getWriteTimes();
		
		try {
			synchronized (deferredOpsQueue) {
				final long now = System.currentTimeMillis();
				final Iterator<Entry<Ids2DatasetEntity, RequestedState>> it = deferredOpsQueue.entrySet().iterator();
				while (it.hasNext()) {
					final Entry<Ids2DatasetEntity, RequestedState> opEntry = it.next();
					final Ids2DatasetEntity ds = opEntry.getKey();
					logger.info("Processing " + ds);
					if (!changing.contains(ds)) {
//						Dataset icatDs = (Dataset) this.icatClient.get(sessionId, "Dataset", ds.getDatasetId());
//						ds.setLocation(icatDs.getLocation());
						final RequestedState state = opEntry.getValue();
						logger.info("Will process " + ds + " with " + state);
						if (state == RequestedState.WRITE_REQUESTED) {
							if (now > writeTimes.get(ds)) {
								writeTimes.remove(ds);
								changing.add(ds);
								it.remove();
								// final Thread w = new Thread(new
								// Writer(location));
								// w.start();
							}
						} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
							if (now > writeTimes.get(ds)) {
								writeTimes.remove(ds);
								changing.add(ds);
								it.remove();
								// final Thread w = new Thread(new
								// WriteThenArchiver(location));
								// w.start();
							}
						} else if (state == RequestedState.ARCHIVE_REQUESTED) {
							changing.add(ds);
							it.remove();
							// final Thread w = new Thread(new
							// Archiver(location));
							// w.start();
						} else if (state == RequestedState.RESTORE_REQUESTED) {
							changing.add(ds);
							it.remove();
							final Thread w = new Thread(new Restorer(ds, requestHelper));
							w.start();
						}
					}
				}
			}
		} finally {
//			logger.info("Starting new Timer from ProcessQueue");
			timer.schedule(new ProcessQueue(timer, requestHelper), 
					PropertyHandler.getInstance().getProcessQueueIntervalSeconds() * 1000L);
		}

	}

}