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

import org.icatproject.Dataset;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.LockManager.LockInfo;
import org.icatproject.ids.LockManager.LockType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.thread.DfArchiver;
import org.icatproject.ids.thread.DfDeleter;
import org.icatproject.ids.thread.DfRestorer;
import org.icatproject.ids.thread.DfWriter;
import org.icatproject.ids.thread.DsArchiver;
import org.icatproject.ids.thread.DsRestorer;
import org.icatproject.ids.thread.DsWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DependsOn({ "LockManager" })
public class FiniteStateMachine {

	private class DfProcessQueue extends TimerTask {

		@Override
		public void run() {
			try {
				synchronized (deferredDfOpsQueue) {
					if (processOpsTime != null && System.currentTimeMillis() > processOpsTime && !deferredDfOpsQueue.isEmpty()) {
						processOpsTime = null;
						logger.debug("deferredDfOpsQueue has " + deferredDfOpsQueue.size() + " entries");
						List<DfInfo> writes = new ArrayList<>();
						List<DfInfo> archives = new ArrayList<>();
						List<DfInfo> restores = new ArrayList<>();
						List<DfInfo> deletes = new ArrayList<>();
						Map<Long, Lock> writeLocks = new HashMap<>();
						Map<Long, Lock> archiveLocks = new HashMap<>();
						Map<Long, Lock> restoreLocks = new HashMap<>();
						Map<Long, Lock> deleteLocks = new HashMap<>();

						Map<DfInfoImpl, RequestedState> newOps = new HashMap<>();
						final Iterator<Entry<DfInfoImpl, RequestedState>> it = deferredDfOpsQueue.entrySet().iterator();
						while (it.hasNext()) {
							Entry<DfInfoImpl, RequestedState> opEntry = it.next();
							DfInfoImpl dfInfo = opEntry.getKey();
							Long dsId = dfInfo.getDsId();
							DsInfo dsInfo;
							try {
								Dataset ds = (Dataset) reader.get("Dataset ds INCLUDE ds.investigation.facility", dsId);
								dsInfo = new DsInfoImpl(ds);
							} catch (Exception e) {
								logger.error("Could not get dsInfo {}: {}.", dsId, e.getMessage());
								continue;
							}
							if (!dfChanging.containsKey(dfInfo)) {
								final RequestedState state = opEntry.getValue();
								logger.debug(dfInfo + " " + state);
								if (state == RequestedState.WRITE_REQUESTED) {
									if (!writeLocks.containsKey(dsId)) {
										try {
											writeLocks.put(dsId, lockManager.lock(dsInfo, LockType.SHARED));
										} catch (AlreadyLockedException e) {
											logger.debug("Could not acquire lock on " + dsId + ", hold back " + state);
											continue;
										} catch (IOException e) {
											logger.error("I/O exception " + e.getMessage() + " locking " + dsId);
											continue;
										}
									}
									it.remove();
									dfChanging.put(dfInfo, state);
									writes.add(dfInfo);
								} else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
									if (!writeLocks.containsKey(dsId)) {
										try {
											writeLocks.put(dsId, lockManager.lock(dsInfo, LockType.SHARED));
										} catch (AlreadyLockedException e) {
											logger.debug("Could not acquire lock on " + dsId + ", hold back " + state);
											continue;
										} catch (IOException e) {
											logger.error("I/O exception " + e.getMessage() + " locking " + dsId);
											continue;
										}
									}
									it.remove();
									dfChanging.put(dfInfo, RequestedState.WRITE_REQUESTED);
									writes.add(dfInfo);
									newOps.put(dfInfo, RequestedState.ARCHIVE_REQUESTED);
								} else if (state == RequestedState.ARCHIVE_REQUESTED) {
									if (!archiveLocks.containsKey(dsId)) {
										try {
											archiveLocks.put(dsId, lockManager.lock(dsInfo, LockType.EXCLUSIVE));
										} catch (AlreadyLockedException e) {
											logger.debug("Could not acquire lock on " + dsId + ", hold back " + state);
											continue;
										} catch (IOException e) {
											logger.error("I/O exception " + e.getMessage() + " locking " + dsId);
											continue;
										}
									}
									it.remove();
									dfChanging.put(dfInfo, state);
									archives.add(dfInfo);
								} else if (state == RequestedState.RESTORE_REQUESTED) {
									if (!restoreLocks.containsKey(dsId)) {
										try {
											restoreLocks.put(dsId, lockManager.lock(dsInfo, LockType.EXCLUSIVE));
										} catch (AlreadyLockedException e) {
											logger.debug("Could not acquire lock on " + dsId + ", hold back " + state);
											continue;
										} catch (IOException e) {
											logger.error("I/O exception " + e.getMessage() + " locking " + dsId);
											continue;
										}
									}
									it.remove();
									dfChanging.put(dfInfo, state);
									restores.add(dfInfo);
								} else if (state == RequestedState.DELETE_REQUESTED) {
									if (!deleteLocks.containsKey(dsId)) {
										try {
											deleteLocks.put(dsId, lockManager.lock(dsInfo, LockType.EXCLUSIVE));
										} catch (AlreadyLockedException e) {
											logger.debug("Could not acquire lock on " + dsId + ", hold back " + state);
											continue;
										} catch (IOException e) {
											logger.error("I/O exception " + e.getMessage() + " locking " + dsId);
											continue;
										}
									}
									it.remove();
									dfChanging.put(dfInfo, state);
									deletes.add(dfInfo);
								} else {
									throw new AssertionError("Impossible state");
								}
							}
						}
						if (!newOps.isEmpty()) {
							deferredDfOpsQueue.putAll(newOps);
							logger.debug("Adding {} operations to be scheduled next time round", newOps.size());
						}
						if (!deferredDfOpsQueue.isEmpty()) {
							processOpsTime = 0L;
						}
						if (!writes.isEmpty()) {
							logger.debug("Launch thread to process " + writes.size() + " writes");
							Thread w = new Thread(new DfWriter(writes, propertyHandler, FiniteStateMachine.this, writeLocks.values()));
							w.start();
						}
						if (!archives.isEmpty()) {
							logger.debug("Launch thread to process " + archives.size() + " archives");
							Thread w = new Thread(new DfArchiver(archives, propertyHandler, FiniteStateMachine.this, archiveLocks.values()));
							w.start();
						}
						if (!restores.isEmpty()) {
							logger.debug("Launch thread to process " + restores.size() + " restores");
							Thread w = new Thread(new DfRestorer(restores, propertyHandler, FiniteStateMachine.this, restoreLocks.values()));
							w.start();
						}
						if (!deletes.isEmpty()) {
							logger.debug("Launch thread to process " + deletes.size() + " deletes");
							Thread w = new Thread(new DfDeleter(deletes, propertyHandler, FiniteStateMachine.this, deleteLocks.values()));
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
					Map<DsInfo, RequestedState> newOps = new HashMap<>();
					final Iterator<Entry<DsInfo, RequestedState>> it = deferredDsOpsQueue.entrySet().iterator();
					while (it.hasNext()) {
						final Entry<DsInfo, RequestedState> opEntry = it.next();
						final DsInfo dsInfo = opEntry.getKey();
						if (!dsChanging.containsKey(dsInfo)) {
							final RequestedState state = opEntry.getValue();
							if (state == RequestedState.WRITE_REQUESTED
									|| state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
								if (now > writeTimes.get(dsInfo)) {
									try {
										Lock lock = lockManager.lock(dsInfo, LockType.SHARED);
										logger.debug("Will process " + dsInfo + " with " + state);
										writeTimes.remove(dsInfo);
										dsChanging.put(dsInfo, RequestedState.WRITE_REQUESTED);
										it.remove();
										final Thread w = new Thread(
													    new DsWriter(dsInfo, propertyHandler, FiniteStateMachine.this, reader, lock));
										w.start();
										if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
											newOps.put(dsInfo, RequestedState.ARCHIVE_REQUESTED);
										}
									} catch (AlreadyLockedException e) {
										logger.debug("Could not acquire lock on " + dsInfo + ", hold back process with " + state);
									} catch (IOException e) {
										logger.error("I/O exception " + e.getMessage() + " locking " + dsInfo);
									}
								}
							} else if (state == RequestedState.ARCHIVE_REQUESTED) {
								try {
									Lock lock = lockManager.lock(dsInfo, LockType.EXCLUSIVE);
									it.remove();
									long dsId = dsInfo.getDsId();
									logger.debug("Will process " + dsInfo + " with " + state);
									dsChanging.put(dsInfo, state);
									final Thread w = new Thread(
												    new DsArchiver(dsInfo, propertyHandler, FiniteStateMachine.this, lock));
									w.start();
								} catch (AlreadyLockedException e) {
									logger.debug("Could not acquire lock on " + dsInfo + ", hold back process with " + state);
								} catch (IOException e) {
									logger.error("I/O exception " + e.getMessage() + " locking " + dsInfo);
								}
							} else if (state == RequestedState.RESTORE_REQUESTED) {
								try {
									Lock lock = lockManager.lock(dsInfo, LockType.EXCLUSIVE);
									logger.debug("Will process " + dsInfo + " with " + state);
									dsChanging.put(dsInfo, state);
									it.remove();
									final Thread w = new Thread(
												    new DsRestorer(dsInfo, propertyHandler, FiniteStateMachine.this, reader, lock));
									w.start();
								} catch (AlreadyLockedException e) {
									logger.debug("Could not acquire lock on " + dsInfo + ", hold back process with " + state);
								} catch (IOException e) {
									logger.error("I/O exception " + e.getMessage() + " locking " + dsInfo);
								}
							}
						}
					}
					deferredDsOpsQueue.putAll(newOps);
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

	/*
	 * Note that the veriable processOpsDelayMillis is used to either delay all deferred
	 * datafile operations or to delay dataset writes, depending on the setting of storageUnit.
	 */
	private long processOpsDelayMillis;

	private Map<DfInfoImpl, RequestedState> deferredDfOpsQueue = new HashMap<>();

	private Map<DsInfo, RequestedState> deferredDsOpsQueue = new HashMap<>();

	private Map<DfInfo, RequestedState> dfChanging = new HashMap<>();

	private Map<DsInfo, RequestedState> dsChanging = new HashMap<>();

	private Path markerDir;
	private long processQueueIntervalMillis;

	private PropertyHandler propertyHandler;
	@EJB
	IcatReader reader;

	@EJB
	private LockManager lockManager;

	private StorageUnit storageUnit;

	private Timer timer = new Timer("FSM Timer");

	private Long processOpsTime;

	private Map<DsInfo, Long> writeTimes = new HashMap<>();

	private Set<Long> failures = ConcurrentHashMap.newKeySet();

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
		try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
			if (storageUnit == null) {
				gen.writeStartArray("opsQueue").writeEnd();
			} else if (storageUnit == StorageUnit.DATASET) {
				Map<DsInfo, RequestedState> union;
				synchronized (deferredDsOpsQueue) {
					union = new HashMap<>(dsChanging);
					union.putAll(deferredDsOpsQueue);
				}
				gen.writeStartArray("opsQueue");
				for (Entry<DsInfo, RequestedState> entry : union.entrySet()) {
					DsInfo item = entry.getKey();
					gen.writeStartObject().write("data", item.toString()).write("request", entry.getValue().name())
							.writeEnd();
				}
				gen.writeEnd(); // end Array("opsQueue")
			} else if (storageUnit == StorageUnit.DATAFILE) {
				Map<DfInfo, RequestedState> union;
				synchronized (deferredDfOpsQueue) {
					union = new HashMap<>(dfChanging);
					union.putAll(deferredDfOpsQueue);
				}
				gen.writeStartArray("opsQueue");
				for (Entry<DfInfo, RequestedState> entry : union.entrySet()) {
					DfInfo item = entry.getKey();
					gen.writeStartObject().write("data", item.toString()).write("request", entry.getValue().name())
							.writeEnd();
				}
				gen.writeEnd(); // end Array("opsQueue")
			}

			Collection<LockInfo> lockInfo = lockManager.getLockInfo();
			gen.write("lockCount", lockInfo.size());
			gen.writeStartArray("locks");
			for (LockInfo li : lockInfo) {
				gen.writeStartObject().write("id", li.id).write("type", li.type.name()).write("count", li.count).writeEnd();
			}
			gen.writeEnd(); // end Array("locks")

			gen.writeStartArray("failures");
			for (Long failure : failures) {
				gen.write(failure);
			}
			gen.writeEnd(); // end Array("failures")

			gen.writeEnd(); // end Object()
		}
		return baos.toString();
	}

	@PostConstruct
	private void init() {
		try {
			propertyHandler = PropertyHandler.getInstance();
			processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;
			storageUnit = propertyHandler.getStorageUnit();
			if (storageUnit == StorageUnit.DATASET) {
				processOpsDelayMillis = propertyHandler.getDelayDatasetWrites() * 1000L;
				timer.schedule(new DsProcessQueue(), processQueueIntervalMillis);
				logger.info("DsProcessQueue scheduled to run in " + processQueueIntervalMillis + " milliseconds");
			} else if (storageUnit == StorageUnit.DATAFILE) {
				processOpsDelayMillis = propertyHandler.getDelayDatafileOperations() * 1000L;
				timer.schedule(new DfProcessQueue(), processQueueIntervalMillis);
				logger.info("DfProcessQueue scheduled to run in " + processQueueIntervalMillis + " milliseconds");
			}
			markerDir = propertyHandler.getCacheDir().resolve("marker");
			Files.createDirectories(markerDir);
		} catch (IOException e) {
			throw new RuntimeException("FiniteStateMachine reports " + e.getClass() + " " + e.getMessage());
		}
	}

	public void queue(DfInfoImpl dfInfo, DeferredOp deferredOp) throws InternalException {
		logger.info("Requesting " + deferredOp + " of datafile " + dfInfo);

		synchronized (deferredDfOpsQueue) {

			if (processOpsTime == null) {
				processOpsTime = System.currentTimeMillis() + processOpsDelayMillis;
				final Date d = new Date(processOpsTime);
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
		writeTimes.put(dsInfo, System.currentTimeMillis() + processOpsDelayMillis);
		if (logger.isDebugEnabled()) {
			final Date d = new Date(writeTimes.get(dsInfo));
			logger.debug("Requesting delay of writing of dataset " + dsInfo + " till " + d);
		}
	}

	public void recordSuccess(Long id) {
		if (failures.remove(id)) {
			logger.debug("Marking {} OK", id);
		}
	}

	public void recordFailure(Long id) {
		if (failures.add(id)) {
			logger.debug("Marking {} as failure", id);
		}
	}

	/**
	 * Check whether the Dataset/Datafile ID (depending on the StorageUnit set)
	 * is in the list of IDs that failed to restore. The behaviour then depends 
	 * on whether the property allowRestoreFailures is set. 
	 * 
	 * @param id a Dataset or Datafile ID
	 * @return true if the ID is found in the list of failures and 
	 *         allowRestoreFailures is set, false if the ID is not found
	 * @throws InternalException if the ID is found in the list of failures
	 *                           and allowRestoreFailures is not set
	 */
	public boolean checkFailure(Long id) throws InternalException {
		if (failures.contains(id)) {
			if (propertyHandler.getAllowRestoreFailures()) {
				return true;
			} else {
				throw new InternalException("Restore failed for " +
						propertyHandler.getStorageUnit().name() + " " + id);
			}
	 	}
	 	return false;
 	}

}
