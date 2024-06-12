package org.icatproject.ids.finiteStateMachine;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import java.util.TimerTask;

import org.icatproject.Dataset;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.models.DatafileInfo;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.DatasetInfo;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager;
import org.icatproject.ids.services.LockManager.Lock;
import org.icatproject.ids.services.LockManager.LockType;
import org.icatproject.ids.thread.DfArchiver;
import org.icatproject.ids.thread.DfDeleter;
import org.icatproject.ids.thread.DfRestorer;
import org.icatproject.ids.thread.DfWriter;

import jakarta.json.stream.JsonGenerator;

public class FiniteStateMachineForStorageUnitDatafile
        extends FiniteStateMachine {

    protected FiniteStateMachineForStorageUnitDatafile(IcatReader icatReader,
            LockManager lockManager) {
        super(icatReader, lockManager);
    }

    @Override
    protected void scheduleTimer() {
        processOpsDelayMillis = propertyHandler.getDelayDatafileOperations()
                * 1000L;
        timer.schedule(new DfProcessQueue(), processQueueIntervalMillis);
        logger.info("DfProcessQueue scheduled to run in "
                + processQueueIntervalMillis + " milliseconds");
    }

    @Override
    protected void addDataInfoJson(JsonGenerator gen) {
        this.addDataInfoJsonFromDeferredOpsQueue(gen);
    }

    public void queue(DataInfoBase dataInfo, DeferredOp deferredOp)
            throws InternalException {

        var dfInfo = (DatafileInfo) dataInfo;
        if (dfInfo == null)
            throw new InternalException(
                    "DataInfoBase object could not be casted into a DataFileInfo. Did you handed over a DataSetInfo instead?");

        logger.info("Requesting " + deferredOp + " of datafile " + dfInfo);

        synchronized (deferredOpsQueue) {

            if (processOpsTime == null) {
                processOpsTime = System.currentTimeMillis()
                        + processOpsDelayMillis;
                final Date d = new Date(processOpsTime);
                logger.debug("Requesting delay operations till " + d);
            }

            final RequestedState state = this.deferredOpsQueue.get(dfInfo);
            if (state == null) {
                if (deferredOp == DeferredOp.WRITE) {
                    try {
                        Path marker = markerDir
                                .resolve(Long.toString(dfInfo.getId()));
                        Files.createFile(marker);
                        logger.debug("Created marker " + marker);
                    } catch (FileAlreadyExistsException e) {
                        // Pass will ignore this
                    } catch (IOException e) {
                        throw new InternalException(
                                e.getClass() + " " + e.getMessage());
                    }
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.WRITE_REQUESTED);
                } else if (deferredOp == DeferredOp.ARCHIVE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.ARCHIVE_REQUESTED);
                } else if (deferredOp == DeferredOp.RESTORE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.RESTORE_REQUESTED);
                } else if (deferredOp == DeferredOp.DELETE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.DELETE_REQUESTED);
                }
            } else if (state == RequestedState.ARCHIVE_REQUESTED) {
                if (deferredOp == DeferredOp.RESTORE) {
                    deferredOpsQueue.remove(dfInfo);
                } else if (deferredOp == DeferredOp.DELETE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.DELETE_REQUESTED);
                }
            } else if (state == RequestedState.DELETE_REQUESTED) {
                // No way out
            } else if (state == RequestedState.RESTORE_REQUESTED) {
                if (deferredOp == DeferredOp.DELETE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.DELETE_REQUESTED);
                } else if (deferredOp == DeferredOp.ARCHIVE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.ARCHIVE_REQUESTED);
                }
            } else if (state == RequestedState.WRITE_REQUESTED) {
                if (deferredOp == DeferredOp.DELETE) {
                    deferredOpsQueue.remove(dfInfo);
                } else if (deferredOp == DeferredOp.ARCHIVE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
                }
            } else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
                if (deferredOp == DeferredOp.DELETE) {
                    deferredOpsQueue.remove(dfInfo);
                } else if (deferredOp == DeferredOp.RESTORE) {
                    deferredOpsQueue.put(dfInfo,
                            RequestedState.WRITE_REQUESTED);
                }
            }
        }
    }

    private class DfProcessQueue extends TimerTask {

        @Override
        public void run() {
            try {
                synchronized (deferredOpsQueue) {
                    if (processOpsTime != null
                            && System.currentTimeMillis() > processOpsTime
                            && !deferredOpsQueue.isEmpty()) {
                        processOpsTime = null;
                        logger.debug("deferredDfOpsQueue has "
                                + deferredOpsQueue.size() + " entries");
                        List<DatafileInfo> writes = new ArrayList<>();
                        List<DatafileInfo> archives = new ArrayList<>();
                        List<DatafileInfo> restores = new ArrayList<>();
                        List<DatafileInfo> deletes = new ArrayList<>();
                        Map<Long, Lock> writeLocks = new HashMap<>();
                        Map<Long, Lock> archiveLocks = new HashMap<>();
                        Map<Long, Lock> restoreLocks = new HashMap<>();
                        Map<Long, Lock> deleteLocks = new HashMap<>();

                        Map<DataInfoBase, RequestedState> newOps = new HashMap<>();
                        final Iterator<Entry<DataInfoBase, RequestedState>> it = deferredOpsQueue
                                .entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<DataInfoBase, RequestedState> opEntry = it
                                    .next();
                            var dfInfo = (DatafileInfo) opEntry.getKey();

                            if (dfInfo == null)
                                throw new RuntimeException(
                                        "Could not cast DataInfoBase to DataFileInfo. Did you handed over another sub type?");

                            Long dsId = dfInfo.getDsId();
                            DatasetInfo dsInfo;
                            try {
                                Dataset ds = (Dataset) reader.get(
                                        "Dataset ds INCLUDE ds.investigation.facility",
                                        dsId);
                                dsInfo = new DatasetInfo(ds);
                            } catch (Exception e) {
                                logger.error("Could not get dsInfo {}: {}.",
                                        dsId, e.getMessage());
                                continue;
                            }
                            if (!dataInfoChanging.containsKey(dfInfo)) {
                                final RequestedState state = opEntry.getValue();
                                logger.debug(dfInfo + " " + state);
                                if (state == RequestedState.WRITE_REQUESTED) {
                                    if (!writeLocks.containsKey(dsId)) {
                                        try {
                                            writeLocks.put(dsId,
                                                    lockManager.lock(dsInfo,
                                                            LockType.SHARED));
                                        } catch (AlreadyLockedException e) {
                                            logger.debug(
                                                    "Could not acquire lock on "
                                                            + dsId
                                                            + ", hold back "
                                                            + state);
                                            continue;
                                        } catch (IOException e) {
                                            logger.error("I/O exception "
                                                    + e.getMessage()
                                                    + " locking " + dsId);
                                            continue;
                                        }
                                    }
                                    it.remove();
                                    dataInfoChanging.put(dfInfo, state);
                                    writes.add(dfInfo);
                                } else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
                                    if (!writeLocks.containsKey(dsId)) {
                                        try {
                                            writeLocks.put(dsId,
                                                    lockManager.lock(dsInfo,
                                                            LockType.SHARED));
                                        } catch (AlreadyLockedException e) {
                                            logger.debug(
                                                    "Could not acquire lock on "
                                                            + dsId
                                                            + ", hold back "
                                                            + state);
                                            continue;
                                        } catch (IOException e) {
                                            logger.error("I/O exception "
                                                    + e.getMessage()
                                                    + " locking " + dsId);
                                            continue;
                                        }
                                    }
                                    it.remove();
                                    dataInfoChanging.put(dfInfo,
                                            RequestedState.WRITE_REQUESTED);
                                    writes.add(dfInfo);
                                    newOps.put(dfInfo,
                                            RequestedState.ARCHIVE_REQUESTED);
                                } else if (state == RequestedState.ARCHIVE_REQUESTED) {
                                    if (!archiveLocks.containsKey(dsId)) {
                                        try {
                                            archiveLocks.put(dsId,
                                                    lockManager.lock(dsInfo,
                                                            LockType.EXCLUSIVE));
                                        } catch (AlreadyLockedException e) {
                                            logger.debug(
                                                    "Could not acquire lock on "
                                                            + dsId
                                                            + ", hold back "
                                                            + state);
                                            continue;
                                        } catch (IOException e) {
                                            logger.error("I/O exception "
                                                    + e.getMessage()
                                                    + " locking " + dsId);
                                            continue;
                                        }
                                    }
                                    it.remove();
                                    dataInfoChanging.put(dfInfo, state);
                                    archives.add(dfInfo);
                                } else if (state == RequestedState.RESTORE_REQUESTED) {
                                    if (!restoreLocks.containsKey(dsId)) {
                                        try {
                                            restoreLocks.put(dsId,
                                                    lockManager.lock(dsInfo,
                                                            LockType.EXCLUSIVE));
                                        } catch (AlreadyLockedException e) {
                                            logger.debug(
                                                    "Could not acquire lock on "
                                                            + dsId
                                                            + ", hold back "
                                                            + state);
                                            continue;
                                        } catch (IOException e) {
                                            logger.error("I/O exception "
                                                    + e.getMessage()
                                                    + " locking " + dsId);
                                            continue;
                                        }
                                    }
                                    it.remove();
                                    dataInfoChanging.put(dfInfo, state);
                                    restores.add(dfInfo);
                                } else if (state == RequestedState.DELETE_REQUESTED) {
                                    if (!deleteLocks.containsKey(dsId)) {
                                        try {
                                            deleteLocks.put(dsId,
                                                    lockManager.lock(dsInfo,
                                                            LockType.EXCLUSIVE));
                                        } catch (AlreadyLockedException e) {
                                            logger.debug(
                                                    "Could not acquire lock on "
                                                            + dsId
                                                            + ", hold back "
                                                            + state);
                                            continue;
                                        } catch (IOException e) {
                                            logger.error("I/O exception "
                                                    + e.getMessage()
                                                    + " locking " + dsId);
                                            continue;
                                        }
                                    }
                                    it.remove();
                                    dataInfoChanging.put(dfInfo, state);
                                    deletes.add(dfInfo);
                                } else {
                                    throw new AssertionError(
                                            "Impossible state");
                                }
                            }
                        }
                        if (!newOps.isEmpty()) {
                            deferredOpsQueue.putAll(newOps);
                            logger.debug(
                                    "Adding {} operations to be scheduled next time round",
                                    newOps.size());
                        }
                        if (!deferredOpsQueue.isEmpty()) {
                            processOpsTime = 0L;
                        }
                        if (!writes.isEmpty()) {
                            logger.debug("Launch thread to process "
                                    + writes.size() + " writes");
                            Thread w = new Thread(new DfWriter(writes,
                                    propertyHandler,
                                    FiniteStateMachineForStorageUnitDatafile.this,
                                    writeLocks.values()));
                            w.start();
                        }
                        if (!archives.isEmpty()) {
                            logger.debug("Launch thread to process "
                                    + archives.size() + " archives");
                            Thread w = new Thread(new DfArchiver(archives,
                                    propertyHandler,
                                    FiniteStateMachineForStorageUnitDatafile.this,
                                    archiveLocks.values()));
                            w.start();
                        }
                        if (!restores.isEmpty()) {
                            logger.debug("Launch thread to process "
                                    + restores.size() + " restores");
                            Thread w = new Thread(new DfRestorer(restores,
                                    propertyHandler,
                                    FiniteStateMachineForStorageUnitDatafile.this,
                                    restoreLocks.values()));
                            w.start();
                        }
                        if (!deletes.isEmpty()) {
                            logger.debug("Launch thread to process "
                                    + deletes.size() + " deletes");
                            Thread w = new Thread(new DfDeleter(deletes,
                                    propertyHandler,
                                    FiniteStateMachineForStorageUnitDatafile.this,
                                    deleteLocks.values()));
                            w.start();
                        }
                    }
                }
            } finally {
                timer.schedule(new DfProcessQueue(),
                        processQueueIntervalMillis);
            }

        }

    }
}
