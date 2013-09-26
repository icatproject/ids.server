package org.icatproject.ids.thread;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.icatproject.Dataset;
import org.icatproject.ids.entity.IdsDataEntity;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.util.RequestHelper;
import org.icatproject.ids.util.RequestQueues;
import org.icatproject.ids.util.RequestedState;
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
		Map<IdsDataEntity, RequestedState> deferredOpsQueue = requestQueues.getDeferredOpsQueue();
		Set<Dataset> changing = requestQueues.getChanging();
		Map<Dataset, Long> writeTimes = requestQueues.getWriteTimes();
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} // TODO remove; for testing purposes
		try {
			synchronized (deferredOpsQueue) {
				final long now = System.currentTimeMillis();
				final Iterator<Entry<IdsDataEntity, RequestedState>> it = deferredOpsQueue.entrySet().iterator();
				while (it.hasNext()) {
					final Entry<IdsDataEntity, RequestedState> opEntry = it.next();
					final IdsDataEntity de = opEntry.getKey();
					logger.info("Processing " + de);
					if (!changing.contains(de)) {
						final RequestedState state = opEntry.getValue();
						logger.info("Will process " + de + " with " + state);
						if (state == RequestedState.WRITE_REQUESTED) {
							if (now > writeTimes.get(de.getIcatDataset())) {
								writeTimes.remove(de.getIcatDataset());
								changing.add(de.getIcatDataset());
								it.remove();
								final Thread w = new Thread(new Writer(de, requestHelper));
								w.start();
							}
						} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
							if (now > writeTimes.get(de.getIcatDataset())) {
								writeTimes.remove(de.getIcatDataset());
								changing.add(de.getIcatDataset());
								it.remove();
								final Thread w = new Thread(new WriteThenArchiver(de, requestHelper));
								w.start();
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
			timer.schedule(new ProcessQueue(timer, requestHelper), PropertyHandler.getInstance()
					.getProcessQueueIntervalSeconds() * 1000L);
		}

	}

}