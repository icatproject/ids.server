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
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.DependsOn;
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
@DependsOn("LoggingConfigurator")
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

						final Iterator<Entry<DfInfoImpl, RequestedState>> it = deferredDfOpsQueue
								.entrySet().iterator();
						while (it.hasNext()) {
							Entry<DfInfoImpl, RequestedState> opEntry = it.next();
							DfInfoImpl dfInfo = opEntry.getKey();
							if (!dfChanging.containsKey(dfInfo)) {
								it.remove();
								final RequestedState state = opEntry.getValue();
								dfChanging.put(dfInfo, state);
								logger.debug(dfInfo + " " + state);
								if (state == RequestedState.WRITE_REQUESTED) {
									writes.add(dfInfo);
								} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
									writeThenArchives.add(dfInfo);
								} else if (state == RequestedState.ARCHIVE_REQUESTED) {
									long dsId = dfInfo.getDsId();
									if (isLocked(dsId, QueryLockType.ARCHIVE)) {
										logger.debug("Archive of " + dfInfo
												+ " skipped because getData in progress");
										continue;
									}
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
						if (!dsChanging.containsKey(dsInfo)) {
							final RequestedState state = opEntry.getValue();
							if (state == RequestedState.WRITE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									dsChanging.put(dsInfo, state);
									it.remove();
									final Thread w = new Thread(new DsWriter(dsInfo,
											propertyHandler, FiniteStateMachine.this, reader));
									w.start();
								}
							} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									logger.debug("Will process " + dsInfo + " with " + state);
									writeTimes.remove(dsInfo);
									dsChanging.put(dsInfo, state);
									it.remove();
									final Thread w = new Thread(new DsWriteThenArchiver(dsInfo,
											propertyHandler, FiniteStateMachine.this, reader));
									w.start();
								}
							} else if (state == RequestedState.ARCHIVE_REQUESTED) {
								it.remove();
								long dsId = dsInfo.getDsId();
								if (isLocked(dsId, QueryLockType.ARCHIVE)) {
									logger.debug("Archive of " + dsInfo
											+ " skipped because getData in progress");
									continue;
								}
								logger.debug("Will process " + dsInfo + " with " + state);
								dsChanging.put(dsInfo, state);
								final Thread w = new Thread(new DsArchiver(dsInfo, propertyHandler,
										FiniteStateMachine.this));
								w.start();
							} else if (state == RequestedState.RESTORE_REQUESTED) {
								logger.debug("Will process " + dsInfo + " with " + state);
								dsChanging.put(dsInfo, state);
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

	public enum SetLockType {
		ARCHIVE, ARCHIVE_AND_DELETE
	}

	public enum QueryLockType {
		ARCHIVE, DELETE
	}

	private static Logger logger = LoggerFactory.getLogger(FiniteStateMachine.class);

	private long archiveWriteDelayMillis;

	private Map<DfInfoImpl, RequestedState> deferredDfOpsQueue = new HashMap<>();

	private Map<DsInfo, RequestedState> deferredDsOpsQueue = new HashMap<>();

	private Map<DfInfo, RequestedState> dfChanging = new HashMap<>();

	private Map<DsInfo, RequestedState> dsChanging = new HashMap<>();

	private Map<String, Set<Long>> deleteLocks = new HashMap<>();
	private Map<String, Set<Long>> archiveLocks = new HashMap<>();

	private Path markerDir;
	private long processQueueIntervalMillis;

	private PropertyHandler propertyHandler;
	@EJB
	IcatReader reader;

	private StorageUnit storageUnit;

	private boolean synchLocksOnDataset;

	private Timer timer = new Timer("FSM Timer");

	private Long writeTime;

	private Map<DsInfo, Long> writeTimes = new HashMap<>();

	private Map<Long, String> failures = new ConcurrentHashMap<>();

	@PreDestroy
	private void exit() {
		timer.cancel();
		logger.info("Cancelled timer");
	}

	/**
	 * Find any DfInfo which may be offline
	 */
	public Set<DfInfo> getDfMaybeOffline() {
		Map<DfInfo, RequestedState> union;
		synchronized (deferredDfOpsQueue) {
			union = new HashMap<>(dfChanging);
			union.putAll(deferredDfOpsQueue);
		}
		Set<DfInfo> result = new HashSet<>();
		for (Entry<DfInfo, RequestedState> entry : union.entrySet()) {
			if (entry.getValue() != RequestedState.WRITE_REQUESTED) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	/**
	 * Find any DfInfo which are being restored or are queued for restoration
	 */
	public Set<DfInfo> getDfRestoring() {
		Map<DfInfo, RequestedState> union;
		synchronized (deferredDfOpsQueue) {
			union = new HashMap<>(dfChanging);
			union.putAll(deferredDfOpsQueue);
		}
		Set<DfInfo> result = new HashSet<>();
		for (Entry<DfInfo, RequestedState> entry : union.entrySet()) {
			if (entry.getValue() == RequestedState.RESTORE_REQUESTED) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	/**
	 * Find any DsInfo which may be offline
	 */
	public Set<DsInfo> getDsMaybeOffline() {
		Map<DsInfo, RequestedState> union;
		synchronized (deferredDsOpsQueue) {
			union = new HashMap<>(dsChanging);
			union.putAll(deferredDsOpsQueue);
		}
		Set<DsInfo> result = new HashSet<>();
		for (Entry<DsInfo, RequestedState> entry : union.entrySet()) {
			if (entry.getValue() != RequestedState.WRITE_REQUESTED) {
				result.add(entry.getKey());
			}
		}
		return result;
	}

	/**
	 * Find any DsInfo which are being restored or are queued for restoration
	 */
	public Set<DsInfo> getDsRestoring() {
		Map<DsInfo, RequestedState> union;
		synchronized (deferredDsOpsQueue) {
			union = new HashMap<>(dsChanging);
			union.putAll(deferredDsOpsQueue);
		}
		Set<DsInfo> result = new HashSet<>();
		for (Entry<DsInfo, RequestedState> entry : union.entrySet()) {
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
			Map<DsInfo, RequestedState> union;
			Collection<Set<Long>> locksContentsClone;
			synchronized (deferredDsOpsQueue) {
				union = new HashMap<>(dsChanging);
				union.putAll(deferredDsOpsQueue);
				locksContentsClone = new HashSet<>(archiveLocks.values());
				locksContentsClone.addAll(deleteLocks.values());
			}
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.writeStartArray("opsQueue");
				for (Entry<DsInfo, RequestedState> entry : union.entrySet()) {
					DsInfo item = entry.getKey();

					gen.writeStartObject().write("data", item.toString())
							.write("request", entry.getValue().name()).writeEnd();

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
			Map<DfInfo, RequestedState> union;
			Collection<Set<Long>> locksContentsClone;
			synchronized (deferredDfOpsQueue) {
				union = new HashMap<>(dfChanging);
				union.putAll(deferredDfOpsQueue);
				locksContentsClone = new HashSet<>(archiveLocks.values());
				locksContentsClone.addAll(deleteLocks.values());
			}
			try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
				gen.writeStartArray("opsQueue");
				for (Entry<DfInfo, RequestedState> entry : union.entrySet()) {
					DfInfo item = entry.getKey();
					gen.writeStartObject().write("data", item.toString())
							.write("request", entry.getValue().name()).writeEnd();
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
				synchLocksOnDataset = true;
			} else if (storageUnit == StorageUnit.DATAFILE) {
				timer.schedule(new DfProcessQueue(), processQueueIntervalMillis);
				logger.info("DfProcessQueue scheduled to run in " + processQueueIntervalMillis
						+ " milliseconds");
				synchLocksOnDataset = false;
			} else {
				synchLocksOnDataset = true;
			}
			markerDir = propertyHandler.getCacheDir().resolve("marker");
			Files.createDirectories(markerDir);
		} catch (IOException e) {
			throw new RuntimeException("FiniteStateMachine reports " + e.getClass() + " "
					+ e.getMessage());
		}
	}

	public boolean isLocked(long dsId, QueryLockType lockType) {
		if (synchLocksOnDataset) {
			synchronized (deferredDsOpsQueue) {
				return locked(dsId, lockType);
			}
		} else {
			synchronized (deferredDfOpsQueue) {
				return locked(dsId, lockType);
			}
		}
	}

	public String lock(Set<Long> set, SetLockType lockType) {
		String lockId = UUID.randomUUID().toString();
		if (synchLocksOnDataset) {
			synchronized (deferredDsOpsQueue) {
				archiveLocks.put(lockId, set);
				if (lockType == SetLockType.ARCHIVE_AND_DELETE) {
					deleteLocks.put(lockId, set);
				}
			}
		} else {
			synchronized (deferredDfOpsQueue) {
				archiveLocks.put(lockId, set);
				if (lockType == SetLockType.ARCHIVE_AND_DELETE) {
					deleteLocks.put(lockId, set);
				}
			}
		}
		return lockId;
	}

	private boolean locked(long dsId, QueryLockType lockType) {
		if (lockType == QueryLockType.ARCHIVE) {
			for (Set<Long> lock : archiveLocks.values()) {
				if (lock.contains(dsId)) {
					return true;
				}
			}
		} else if (lockType == QueryLockType.DELETE) {
			for (Set<Long> lock : deleteLocks.values()) {
				if (lock.contains(dsId)) {
					return true;
				}
			}
		}
		return false;
	}

	public void queue(DfInfoImpl dfInfo, DeferredOp deferredOp) throws InternalException {
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

	public void unlock(String lockId, SetLockType lockType) {
		if (synchLocksOnDataset) {
			synchronized (deferredDsOpsQueue) {
				archiveLocks.remove(lockId);
				if (lockType == SetLockType.ARCHIVE_AND_DELETE) {
					deleteLocks.remove(lockId);
				}
			}
		} else {
			synchronized (deferredDfOpsQueue) {
				archiveLocks.remove(lockId);
				if (lockType == SetLockType.ARCHIVE_AND_DELETE) {
					deleteLocks.remove(lockId);
				}
			}
		}
	}

	public void recordSuccess(Long id) {
		failures.remove(id);
	}

	public void recordFailure(Long id, String msg) {
		failures.put(id, msg );
	}

	public void checkFailure(Long id) throws InternalException {
		String msg = failures.get(id);
		if (msg != null){
			throw new InternalException("Restore failed " + msg);
		}
		
	}

}
