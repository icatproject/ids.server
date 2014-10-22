package org.icatproject.ids;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.json.Json;
import javax.json.stream.JsonGenerator;

import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.thread.DfArchiver;
import org.icatproject.ids.thread.DfDeleter;
import org.icatproject.ids.thread.DfRestorer;
import org.icatproject.ids.thread.DfWriteThenArchiver;
import org.icatproject.ids.thread.DfWriter;
import org.icatproject.ids.thread.DsArchiver;
import org.icatproject.ids.thread.DsRestorer;
import org.icatproject.ids.thread.DsWriteThenArchiver;
import org.icatproject.ids.thread.DsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class FiniteStateMachine {

	private class DfProcessQueue extends TimerTask {

		@Override
		public void run() {
			try {
				synchronized (deferredDfOpsQueue) {
					if (writeTime != null && System.currentTimeMillis() > writeTime
							&& !deferredDfOpsQueue.isEmpty()) {
						writeTime = null;
						logger.debug("deferredDfOpsQueue has " + deferredDfOpsQueue.size()
								+ " entries");
						List<DfInfo> writes = new ArrayList<>();
						List<DfInfo> writeThenArchives = new ArrayList<>();
						List<DfInfo> archives = new ArrayList<>();
						List<DfInfo> restores = new ArrayList<>();
						List<DfInfo> deletes = new ArrayList<>();

						final Iterator<Entry<DfInfo, RequestedState>> it = deferredDfOpsQueue
								.entrySet().iterator();
						while (it.hasNext()) {
							Entry<DfInfo, RequestedState> opEntry = it.next();
							DfInfo dfInfo = opEntry.getKey();
							if (!dfChanging.contains(dfInfo)) {
								it.remove();
								dfChanging.add(dfInfo);
								final RequestedState state = opEntry.getValue();
								logger.debug(dfInfo + " " + state);
								if (state == RequestedState.WRITE_REQUESTED) {
									writes.add(dfInfo);
								} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
									writeThenArchives.add(dfInfo);
								} else if (state == RequestedState.ARCHIVE_REQUESTED) {
									archives.add(dfInfo);
								} else if (state == RequestedState.RESTORE_REQUESTED) {
									restores.add(dfInfo);
								} else if (state == RequestedState.DELETE_REQUESTED) {
									deletes.add(dfInfo);
								} else {
									throw new AssertionError("Impossible state");
								}
							}
						}
						if (!writes.isEmpty()) {
							logger.debug("Launch thread to process " + writes.size() + " writes");
							Thread w = new Thread(new DfWriter(writes, propertyHandler,
									FiniteStateMachine.this));
							w.start();
						}
						if (!writeThenArchives.isEmpty()) {
							logger.debug("Launch thread to process " + writeThenArchives.size()
									+ " writeThenArchives");
							Thread w = new Thread(new DfWriteThenArchiver(writeThenArchives,
									propertyHandler, FiniteStateMachine.this));
							w.start();
						}
						if (!archives.isEmpty()) {
							logger.debug("Launch thread to process " + archives.size()
									+ " archives");
							Thread w = new Thread(new DfArchiver(archives, propertyHandler,
									FiniteStateMachine.this));
							w.start();
						}
						if (!restores.isEmpty()) {
							logger.debug("Launch thread to process " + restores.size()
									+ " restores");
							Thread w = new Thread(new DfRestorer(restores, propertyHandler,
									FiniteStateMachine.this));
							w.start();
						}
						if (!deletes.isEmpty()) {
							logger.debug("Launch thread to process " + deletes.size() + " deletes");
							Thread w = new Thread(new DfDeleter(deletes, propertyHandler,
									FiniteStateMachine.this));
							w.start();
						}
					}
				}
			} finally {
				timer.schedule(new DfProcessQueue(), processQueueIntervalMillis);
			}

		}

	}

	private class DsProcessQueue extends TimerTask {

		@Override
		public void run() {
			try {
				synchronized (deferredDsOpsQueue) {
					final long now = System.currentTimeMillis();
					final Iterator<Entry<DsInfo, RequestedState>> it = deferredDsOpsQueue
							.entrySet().iterator();
					while (it.hasNext()) {
						final Entry<DsInfo, RequestedState> opEntry = it.next();
						final DsInfo dsInfo = opEntry.getKey();
						if (!dsChanging.contains(dsInfo)) {
							final RequestedState state = opEntry.getValue();
							if (state == RequestedState.WRITE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									dsChanging.add(dsInfo);
									it.remove();
									final Thread w = new Thread(new DsWriter(dsInfo,
											propertyHandler, FiniteStateMachine.this, reader));
									w.start();
								}
							} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									dsChanging.add(dsInfo);
									it.remove();
									final Thread w = new Thread(new DsWriteThenArchiver(dsInfo,
											propertyHandler, FiniteStateMachine.this, reader));
									w.start();
								}
							} else if (state == RequestedState.ARCHIVE_REQUESTED) {
								it.remove();
								long dsId = dsInfo.getDsId();
								if (isLocked(dsId)) {
									logger.debug("Archive of " + dsInfo
											+ " skipped because getData in progress");
									continue;
								}
								logger.debug("Will process " + dsInfo + " with " + state);
								dsChanging.add(dsInfo);
								final Thread w = new Thread(new DsArchiver(dsInfo, propertyHandler,
										FiniteStateMachine.this));
								w.start();
							} else if (state == RequestedState.RESTORE_REQUESTED) {
								logger.debug("Will process " + dsInfo + " with " + state);
								dsChanging.add(dsInfo);
								it.remove();
								final Thread w = new Thread(new DsRestorer(dsInfo, propertyHandler,
										FiniteStateMachine.this, reader));
								w.start();
							}
						}
					}
				}

			} finally {
				timer.schedule(new DsProcessQueue(), processQueueIntervalMillis);
			}

		}

	}

	public enum RequestedState {
		ARCHIVE_REQUESTED, DELETE_REQUESTED, RESTORE_REQUESTED, WRITE_REQUESTED, WRITE_THEN_ARCHIVE_REQUESTED
	}

	private static Logger logger = LoggerFactory.getLogger(FiniteStateMachine.class);

	private long archiveWriteDelayMillis;

	private Map<DfInfo, RequestedState> deferredDfOpsQueue = new HashMap<>();

	private Map<DsInfo, RequestedState> deferredDsOpsQueue = new HashMap<>();

	private Set<DfInfo> dfChanging = new HashSet<>();

	private Set<DsInfo> dsChanging = new HashSet<>();

	private Map<String, Set<Long>> locks = new HashMap<>();
	private Path markerDir;
	private long processQueueIntervalMillis;

	private PropertyHandler propertyHandler;
	@EJB
	IcatReader reader;

	private StorageUnit storageUnit;

	private Timer timer = new Timer("FSM Timer");

	private Long writeTime;

	private Map<DsInfo, Long> writeTimes = new HashMap<>();

	@PreDestroy
	private void exit() {
		timer.cancel();
		logger.info("Cancelled timer");
	}

	/**
	 * Find any DfInfo which are changing or are queued for restoration
	 */
	public Set<DfInfo> getDfRestoring() {
		Map<DfInfo, RequestedState> deferredOpsQueueClone = new HashMap<>();
		Set<DfInfo> result = null;
		synchronized (deferredDfOpsQueue) {
			deferredOpsQueueClone.putAll(deferredDfOpsQueue);
			result = new HashSet<>(dfChanging);
		}
		for (Entry<DfInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
			if (entry.getValue() == RequestedState.RESTORE_REQUESTED) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	/**
	 * Find any DsInfo which are changing or are queued for restoration
	 */
	public Set<DsInfo> getDsRestoring() {
		Map<DsInfo, RequestedState> deferredOpsQueueClone = new HashMap<>();
		Set<DsInfo> result = null;
		synchronized (deferredDsOpsQueue) {
			deferredOpsQueueClone.putAll(deferredDsOpsQueue);
			result = new HashSet<>(dsChanging);
		}
		for (Entry<DsInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
			if (entry.getValue() == RequestedState.RESTORE_REQUESTED) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	public String getServiceStatus() throws InternalException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		if (storageUnit == null) {
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.writeStartArray("opsQueue").writeEnd();
				gen.write("lockCount", 0);
				gen.writeStartArray("lockedIds").writeEnd();
				gen.writeEnd(); // end Object()
			}
		} else if (storageUnit == StorageUnit.DATASET) {
			Map<DsInfo, RequestedState> deferredOpsQueueClone;
			Set<DsInfo> changingClone;
			Collection<Set<Long>> locksContentsClone;
			synchronized (deferredDsOpsQueue) {
				deferredOpsQueueClone = new HashMap<>(deferredDsOpsQueue);
				changingClone = new HashSet<>(dsChanging);
				locksContentsClone = new HashSet<>(locks.values());
			}
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.writeStartArray("opsQueue");
				for (Entry<DsInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
					DsInfo item = entry.getKey();
					if (!changingClone.contains(item)) {
						gen.writeStartObject().write("data", item.toString())
								.write("request", entry.getValue().name()).writeEnd();
					}
				}
				for (DsInfo item : changingClone) {
					gen.writeStartObject().write("data", item.toString())
							.write("request", "CHANGING").writeEnd();
				}
				gen.writeEnd(); // end Array("opsQueue")

				gen.write("lockCount", locksContentsClone.size());

				Set<Long> lockedDs = new HashSet<>();

				for (Set<Long> entry : locksContentsClone) {
					lockedDs.addAll(entry);
				}
				gen.writeStartArray("lockedIds");
				for (Long dsId : lockedDs) {
					gen.write(dsId);
				}
				gen.writeEnd(); // end Array("lockedDs")

				gen.writeEnd(); // end Object()
			}
		} else if (storageUnit == StorageUnit.DATAFILE) {
			Map<DfInfo, RequestedState> deferredOpsQueueClone;
			Set<DfInfo> changingClone;
			Collection<Set<Long>> locksContentsClone;
			synchronized (deferredDfOpsQueue) {
				deferredOpsQueueClone = new HashMap<>(deferredDfOpsQueue);
				changingClone = new HashSet<>(dfChanging);
				locksContentsClone = new HashSet<>(locks.values());
			}
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.writeStartArray("opsQueue");
				for (Entry<DfInfo, RequestedState> entry : deferredOpsQueueClone.entrySet()) {
					DfInfo item = entry.getKey();
					if (!changingClone.contains(item)) {
						gen.writeStartObject().write("data", item.toString())
								.write("request", entry.getValue().name()).writeEnd();
					}
				}
				for (DfInfo item : changingClone) {
					gen.writeStartObject().write("data", item.toString())
							.write("request", "CHANGING").writeEnd();
				}
				gen.writeEnd(); // end Array("opsQueue")

				gen.write("lockCount", locksContentsClone.size());

				Set<Long> lockedDs = new HashSet<>();

				for (Set<Long> entry : locksContentsClone) {
					lockedDs.addAll(entry);
				}
				gen.writeStartArray("lockedIds");
				for (Long dsId : lockedDs) {
					gen.write(dsId);
				}
				gen.writeEnd(); // end Array("lockedDs")

				gen.writeEnd(); // end Object()
			}
		}
		return baos.toString();

	}

	@PostConstruct
	private void init() {
		try {
			propertyHandler = PropertyHandler.getInstance();
			archiveWriteDelayMillis = propertyHandler.getWriteDelaySeconds() * 1000L;
			processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;
			storageUnit = propertyHandler.getStorageUnit();
			if (storageUnit == StorageUnit.DATASET) {
				timer.schedule(new DsProcessQueue(), processQueueIntervalMillis);
				logger.info("DsProcessQueue scheduled to run in " + processQueueIntervalMillis
						+ " milliseconds");
			} else if (storageUnit == StorageUnit.DATAFILE) {
				timer.schedule(new DfProcessQueue(), processQueueIntervalMillis);
				logger.info("DfProcessQueue scheduled to run in " + processQueueIntervalMillis
						+ " milliseconds");
			}
			markerDir = propertyHandler.getCacheDir().resolve("marker");
			Files.createDirectories(markerDir);
		} catch (IOException e) {
			throw new RuntimeException("FiniteStateMachine reports " + e.getClass() + " "
					+ e.getMessage());
		}
	}

	public boolean isLocked(long dsId) {
		synchronized (deferredDsOpsQueue) {
			for (Set<Long> lock : locks.values()) {
				if (lock.contains(dsId)) {
					return true;
				}
			}
			return false;
		}
	}

	public String lock(Set<Long> set) {
		String lockId = UUID.randomUUID().toString();
		synchronized (deferredDsOpsQueue) {
			locks.put(lockId, set);
		}
		return lockId;
	}

	public void queue(DfInfo dfInfo, DeferredOp deferredOp) throws InternalException {
		logger.info("Requesting " + deferredOp + " of datafile " + dfInfo);

		synchronized (deferredDfOpsQueue) {

			if (writeTime == null) {
				writeTime = System.currentTimeMillis() + archiveWriteDelayMillis;
				final Date d = new Date(writeTime);
				logger.debug("Requesting delay operations till " + d);
			}

			final RequestedState state = this.deferredDfOpsQueue.get(dfInfo);
			if (state == null) {
				if (deferredOp == DeferredOp.WRITE) {
					try {
						Path marker = markerDir.resolve(Long.toString(dfInfo.getDfId()));
						Files.createFile(marker);
						logger.debug("Created marker " + marker);
					} catch (FileAlreadyExistsException e) {
						// Pass will ignore this
					} catch (IOException e) {
						throw new InternalException(e.getClass() + " " + e.getMessage());
					}
					deferredDfOpsQueue.put(dfInfo, RequestedState.WRITE_REQUESTED);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.RESTORE_REQUESTED);
				} else if (deferredOp == DeferredOp.DELETE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.DELETE_REQUESTED);
				}
			} else if (state == RequestedState.ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.RESTORE) {
					deferredDfOpsQueue.remove(dfInfo);
				} else if (deferredOp == DeferredOp.DELETE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.DELETE_REQUESTED);
				}
			} else if (state == RequestedState.DELETE_REQUESTED) {
				// No way out
			} else if (state == RequestedState.RESTORE_REQUESTED) {
				if (deferredOp == DeferredOp.DELETE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.DELETE_REQUESTED);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_REQUESTED) {
				if (deferredOp == DeferredOp.DELETE) {
					deferredDfOpsQueue.remove(dfInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.DELETE) {
					deferredDfOpsQueue.remove(dfInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredDfOpsQueue.put(dfInfo, RequestedState.WRITE_REQUESTED);
				}
			}
		}
	}

	public void queue(DsInfo dsInfo, DeferredOp deferredOp) throws InternalException {
		logger.info("Requesting " + deferredOp + " of dataset " + dsInfo);

		synchronized (deferredDsOpsQueue) {

			final RequestedState state = this.deferredDsOpsQueue.get(dsInfo);
			if (state == null) {
				if (deferredOp == DeferredOp.WRITE) {
					requestWrite(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredDsOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredDsOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					requestWrite(dsInfo);
					deferredDsOpsQueue.put(dsInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredDsOpsQueue.put(dsInfo, RequestedState.RESTORE_REQUESTED);
				}
			} else if (state == RequestedState.RESTORE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					requestWrite(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredDsOpsQueue.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.ARCHIVE) {
					deferredDsOpsQueue.put(dsInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
				}
			} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
				if (deferredOp == DeferredOp.WRITE) {
					setDelay(dsInfo);
				} else if (deferredOp == DeferredOp.RESTORE) {
					deferredDsOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
				}
			}
		}

	}

	public void removeFromChanging(DfInfo dfInfo) {
		synchronized (deferredDfOpsQueue) {
			dfChanging.remove(dfInfo);
		}
	}

	public void removeFromChanging(DsInfo dsInfo) {
		synchronized (deferredDsOpsQueue) {
			dsChanging.remove(dsInfo);
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
		deferredDsOpsQueue.put(dsInfo, RequestedState.WRITE_REQUESTED);
		setDelay(dsInfo);
	}

	private void setDelay(DsInfo dsInfo) {
		writeTimes.put(dsInfo, System.currentTimeMillis() + archiveWriteDelayMillis);
		if (logger.isDebugEnabled()) {
			final Date d = new Date(writeTimes.get(dsInfo));
			logger.debug("Requesting delay of writing of dataset " + dsInfo + " till " + d);
		}
	}

	public void unlock(String lockId) {
		synchronized (deferredDsOpsQueue) {
			locks.remove(lockId);
		}
	}

}
