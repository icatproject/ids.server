package org.icatproject.ids;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import javax.ejb.EJB;
import javax.ejb.Singleton;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.thread.Archiver;
import org.icatproject.ids.thread.Preparer;
import org.icatproject.ids.thread.Preparer.PreparerStatus;
import org.icatproject.ids.thread.Restorer;
import org.icatproject.ids.thread.WriteThenArchiver;
import org.icatproject.ids.thread.Writer;
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

	private Path markerDir;
	
	@EJB
	IcatReader reader;

	@PostConstruct
	private void init() {
		try {
			propertyHandler = PropertyHandler.getInstance();
			preparedCount = propertyHandler.getPreparedCount();
			archiveWriteDelayMillis = propertyHandler.getWriteDelaySeconds() * 1000L;
			processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;
			timer.schedule(new ProcessQueue(), processQueueIntervalMillis);
			markerDir = propertyHandler.getCacheDir().resolve("marker");
			Files.createDirectories(markerDir);
		} catch (IOException e) {
			throw new RuntimeException("IdsBean reports " + e.getClass() + " " + e.getMessage());
		}
	}

	private long archiveWriteDelayMillis;

	private final static Logger logger = LoggerFactory.getLogger(FiniteStateMachine.class);

	private final Set<DsInfo> changing = new HashSet<>();
	private final Map<DsInfo, Long> writeTimes = new HashMap<>();

	private final Map<DsInfo, RequestedState> deferredOpsQueue = new HashMap<>();

	public void queue(DsInfo dsInfo, DeferredOp deferredOp) throws InternalException {
		logger.info("Requesting " + deferredOp + " of " + dsInfo);

		synchronized (deferredOpsQueue) {

			final RequestedState state = this.deferredOpsQueue.get(dsInfo);
			if (state == null) {
				if (deferredOp == DeferredOp.WRITE) {
					requestWrite(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					requestWrite(dsInfo);
					deferredOpsQueue.put(dsInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.RESTORE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					requestWrite(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredOpsQueue.put(dsInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
				}
			}
		}

	}

	private void requestWrite(DsInfo dsInfo) throws InternalException {
		try {
			Path marker = markerDir.resolve(Long.toString(dsInfo.getDsId()));
			Files.createFile(marker);
			logger.debug("Created marker " + marker);
		} catch (FileAlreadyExistsException e) {
			// Pass will ignore this
		} catch (IOException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
		deferredOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
		setDelay(dsInfo);
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
											FiniteStateMachine.this, reader));
									w.start();
								}
							} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									changing.add(dsInfo);
									it.remove();
									final Thread w = new Thread(new WriteThenArchiver(dsInfo,
											propertyHandler, FiniteStateMachine.this, reader));
									w.start();
								}
							} else if (state == RequestedState.ARCHIVE_REQUESTED) {
								logger.debug("Will process " + dsInfo + " with " + state);
								changing.add(dsInfo);
								it.remove();
								final Thread w = new Thread(new Archiver(dsInfo, propertyHandler,
										FiniteStateMachine.this));
								w.start();
							} else if (state == RequestedState.RESTORE_REQUESTED) {
								logger.debug("Will process " + dsInfo + " with " + state);
								changing.add(dsInfo);
								it.remove();
								final Thread w = new Thread(new Restorer(dsInfo, propertyHandler,
										FiniteStateMachine.this, reader));
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
		Set<DsInfo> changingClone = null;
		synchronized (deferredOpsQueue) {
			deferredOpsQueueClone.putAll(deferredOpsQueue);
			preparersClone.putAll(preparers);
			changingClone = new HashSet<>(changing);
		}
		ObjectMapper om = new ObjectMapper();
		ObjectNode serviceStatus = om.createObjectNode();

		ArrayNode opsQueue = om.createArrayNode();
		serviceStatus.put("opsQueue", opsQueue);
		for (Entry<DsInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
			DsInfo item = entry.getKey();
			if (!changingClone.contains(item)) {
				ObjectNode queueItem = om.createObjectNode();
				opsQueue.add(queueItem);
				queueItem.put("dsInfo", item.toString());
				queueItem.put("request", entry.getValue().name());
			}
		}
		for (DsInfo item : changingClone) {
			ObjectNode queueItem = om.createObjectNode();
			opsQueue.add(queueItem);
			queueItem.put("dsInfo", item.toString());
			queueItem.put("request", "CHANGING");
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

	/**
	 * Find any DsInfo which are changing or are queued for restoration
	 */
	public Set<DsInfo> getRestoring() {
		Map<DsInfo, RequestedState> deferredOpsQueueClone = new HashMap<>();
		Set<DsInfo> result = null;
		synchronized (deferredOpsQueue) {
			deferredOpsQueueClone.putAll(deferredOpsQueue);
			result = new HashSet<>(changing);
		}
		for (Entry<DsInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
			if (entry.getValue() == RequestedState.RESTORE_REQUESTED) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	public String preparing(DsInfo dsInfo) {

		Map<String, Preparer> preparersClone = new LinkedHashMap<String, Preparer>();

		synchronized (deferredOpsQueue) {
			preparersClone.putAll(preparers);
		}
		for (Entry<String, Preparer> entry : preparersClone.entrySet()) {
			if (entry.getValue().using(dsInfo)) {
				return entry.getKey();
			}

		}
		return null;
	}
}
