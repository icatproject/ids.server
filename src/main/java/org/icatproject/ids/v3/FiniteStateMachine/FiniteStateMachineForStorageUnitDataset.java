package org.icatproject.ids.v3.FiniteStateMachine;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;

import org.icatproject.ids.IcatReader;
import org.icatproject.ids.LockManager;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.LockManager.LockType;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.thread.DsArchiver;
import org.icatproject.ids.thread.DsRestorer;
import org.icatproject.ids.thread.DsWriter;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;

public class FiniteStateMachineForStorageUnitDataset extends FiniteStateMachine {

    protected FiniteStateMachineForStorageUnitDataset(IcatReader reader, LockManager lockManager) {
        super(reader, lockManager);
    }


    @Override
    protected void scheduleTimer() {
        processOpsDelayMillis = propertyHandler.getDelayDatasetWrites() * 1000L;
        timer.schedule(new DsProcessQueue(), processQueueIntervalMillis);
        logger.info("DsProcessQueue scheduled to run in " + processQueueIntervalMillis + " milliseconds");
    }


    public void queue(DataInfoBase dataInfo, DeferredOp deferredOp) throws InternalException {

        var dsInfo = (DataSetInfo) dataInfo;
        if(dsInfo == null) throw new InternalException("DataInfoBase object could not be casted into a DataSetInfo. Did you handed over a DataFileInfo instead?");


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


    private void requestWrite(DataSetInfo dsInfo) throws InternalException {
        try {
            Path marker = markerDir.resolve(Long.toString(dsInfo.getId()));
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


    private class DsProcessQueue extends TimerTask {

        @Override
        public void run() {
            try {
                synchronized (deferredDsOpsQueue) {
                    final long now = System.currentTimeMillis();
                    Map<DataSetInfo, RequestedState> newOps = new HashMap<>();
                    final Iterator<Entry<DataSetInfo, RequestedState>> it = deferredDsOpsQueue.entrySet().iterator();
                    while (it.hasNext()) {
                        final Entry<DataSetInfo, RequestedState> opEntry = it.next();
                        final DataSetInfo dsInfo = opEntry.getKey();
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
                                                new DsWriter(dsInfo, propertyHandler, FiniteStateMachineForStorageUnitDataset.this, reader, lock));
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
                                    logger.debug("Will process " + dsInfo + " with " + state);
                                    dsChanging.put(dsInfo, state);
                                    final Thread w = new Thread(
                                            new DsArchiver(dsInfo, propertyHandler, FiniteStateMachineForStorageUnitDataset.this, lock));
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
                                            new DsRestorer(dsInfo, propertyHandler, FiniteStateMachineForStorageUnitDataset.this, reader, lock));
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

}