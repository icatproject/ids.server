package org.icatproject.ids.webservice;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.PostConstruct;
import javax.ejb.Singleton;

import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.thread.Preparer;
import org.icatproject.ids.thread.Preparer.PreparerStatus;
import org.icatproject.ids.thread.Restorer;
import org.icatproject.ids.thread.WriteThenArchiver;
import org.icatproject.ids.thread.Writer;
import org.icatproject.ids.util.PropertyHandler;
import org.icatproject.ids.webservice.exceptions.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Singleton
public class FiniteStateMachine {

	private PropertyHandler propertyHandler;

	private long processQueueIntervalMillis;

	private int preparedCount;

	@PostConstruct
	private void init() {
		propertyHandler = PropertyHandler.getInstance();
		preparedCount = propertyHandler.getPreparedCount();
		archiveWriteDelayMillis = propertyHandler.getWriteDelaySeconds() * 1000L;
		processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;
		timer.schedule(new ProcessQueue(), processQueueIntervalMillis);
	}

	private long archiveWriteDelayMillis;

	private final static Logger logger = LoggerFactory.getLogger(FiniteStateMachine.class);

	private final Set<DsInfo> changing = new HashSet<>();
	private final Map<DsInfo, Long> writeTimes = new HashMap<>();

	private final Map<DsInfo, RequestedState> deferredOpsQueue = new HashMap<>();

	public void queue(DsInfo dsInfo, DeferredOp deferredOp) {
		logger.info("Requesting " + deferredOp + " of " + dsInfo);

		synchronized (deferredOpsQueue) {

			final RequestedState state = this.deferredOpsQueue.get(dsInfo);
			if (state == null) {
				if (deferredOp == DeferredOp.WRITE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.RESTORE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					this.setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					this.deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
				}
			}
		}
	}

	private void setDelay(DsInfo dsInfo) {
		writeTimes.put(dsInfo, System.currentTimeMillis() + archiveWriteDelayMillis);
		if (logger.isDebugEnabled()) {
			final Date d = new Date(writeTimes.get(dsInfo));
			logger.debug("Requesting delay of writing of " + dsInfo + " till " + d);
		}
	}

	private class ProcessQueue extends TimerTask {

		@Override
		public void run() {
			try {
				synchronized (deferredOpsQueue) {
					final long now = System.currentTimeMillis();
					final Iterator<Entry<DsInfo, RequestedState>> it = deferredOpsQueue.entrySet()
							.iterator();
					while (it.hasNext()) {
						final Entry<DsInfo, RequestedState> opEntry = it.next();
						final DsInfo dsInfo = opEntry.getKey();
						if (!changing.contains(dsInfo)) {
							final RequestedState state = opEntry.getValue();
							if (state == RequestedState.WRITE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									changing.add(dsInfo);
									it.remove();
									final Thread w = new Thread(new Writer(dsInfo, propertyHandler,
											FiniteStateMachine.this));
									w.start();
								}
							} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									changing.add(dsInfo);
									it.remove();
									final Thread w = new Thread(new WriteThenArchiver(dsInfo,
											propertyHandler, FiniteStateMachine.this));
									w.start();
								}
							} else if (state == RequestedState.ARCHIVE_REQUESTED) {
								logger.debug("Will process " + dsInfo + " with " + state);
								changing.add(dsInfo);
								it.remove();
								final Thread w = new Thread(
										new org.icatproject.ids.thread.Archiver(dsInfo,
												propertyHandler, FiniteStateMachine.this));
								w.start();
							} else if (state == RequestedState.RESTORE_REQUESTED) {
								logger.debug("Will process " + dsInfo + " with " + state);
								changing.add(dsInfo);
								it.remove();
								final Thread w = new Thread(new Restorer(dsInfo, propertyHandler,
										FiniteStateMachine.this));
								w.start();
							}
						}
					}
					/* Clean out completed ones */
					Iterator<Entry<String, Preparer>> iter = preparers.entrySet().iterator();
					while (iter.hasNext()) {
						Entry<String, Preparer> e = iter.next();
						if (e.getValue().getStatus() == PreparerStatus.COMPLETE) {
							iter.remove();
							logger.debug("Removed complete " + e.getKey() + " from Preparers map");
						}
					}
					/* Clean out old ones if too many */
					iter = preparers.entrySet().iterator();
					while (iter.hasNext()) {
						Entry<String, Preparer> e = iter.next();
						if (preparers.size() <= preparedCount) {
							break;
						}
						iter.remove();
						logger.debug("Removed overflowing" + e.getKey() + " from Preparers map");
					}
				}

			} finally {
				timer.schedule(new ProcessQueue(), processQueueIntervalMillis);
			}

		}
	}

	public enum RequestedState {
		ARCHIVE_REQUESTED, RESTORE_REQUESTED, WRITE_REQUESTED, WRITE_THEN_ARCHIVE_REQUESTED
	}

	public void removeFromChanging(DsInfo dsInfo) {
		synchronized (deferredOpsQueue) {
			changing.remove(dsInfo);
		}

	}

	private Timer timer = new Timer();

	private Map<String, Preparer> preparers = new LinkedHashMap<String, Preparer>();

	public void registerPreparer(String preparedId, Preparer preparer) {
		synchronized (deferredOpsQueue) {
			preparers.put(preparedId, preparer);
		}
	}

	public Preparer getPreparer(String preparedId) {
		synchronized (deferredOpsQueue) {
			return preparers.get(preparedId);
		}
	}

	public String getServiceStatus() throws InternalException {
		Map<DsInfo, RequestedState> deferredOpsQueueClone = new HashMap<>();
		Map<String, Preparer> preparersClone = new LinkedHashMap<String, Preparer>();
		synchronized (deferredOpsQueue) {
			deferredOpsQueueClone.putAll(deferredOpsQueue);
			preparersClone.putAll(preparers);
		}
		ObjectMapper om = new ObjectMapper();
		ObjectNode serviceStatus = om.createObjectNode();

		ArrayNode opsQueue = om.createArrayNode();
		serviceStatus.put("opsQueue", opsQueue);
		for (Entry<DsInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
			ObjectNode queueItem = om.createObjectNode();
			opsQueue.add(queueItem);
			queueItem.put("dsInfo", entry.getKey().toString());
			queueItem.put("request", entry.getValue().name());
		}

		ArrayNode prepQueue = om.createArrayNode();
		serviceStatus.put("prepQueue", prepQueue);
		for (Entry<String, Preparer> entry : preparersClone.entrySet()) {
			ObjectNode queueItem = om.createObjectNode();
			prepQueue.add(queueItem);
			queueItem.put("id", entry.getKey());
			queueItem.put("state", entry.getValue().getStatus().name());
		}

		try {
			return om.writeValueAsString(serviceStatus);
		} catch (JsonProcessingException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}
}
