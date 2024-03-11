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
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.DataSetInfo;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.thread.DsArchiver;
import org.icatproject.ids.thread.DsRestorer;
import org.icatproject.ids.thread.DsWriter;

import jakarta.json.stream.JsonGenerator;

public class FiniteStateMachineForStorageUnitDataset extends FiniteStateMachine {

    protected Map<DataInfoBase, Long> writeTimes = new HashMap<>();

    protected FiniteStateMachineForStorageUnitDataset(IcatReader reader, LockManager lockManager) {
        super(reader, lockManager);
    }


    @Override
    protected void scheduleTimer() {
        processOpsDelayMillis = propertyHandler.getDelayDatasetWrites() * 1000L;
        timer.schedule(new DsProcessQueue(), processQueueIntervalMillis);
        logger.info("DsProcessQueue scheduled to run in " + processQueueIntervalMillis + " milliseconds");
    }

    @Override
    protected void addDataInfoJson(JsonGenerator gen) {
        this.addDataInfoJsonFromDeferredOpsQueue(gen);
    }


    public void queue(DataInfoBase dataInfo, DeferredOp deferredOp) throws InternalException {


        logger.info("Requesting " + deferredOp + " of dataset " + dataInfo);

        synchronized (deferredOpsQueue) {

            final RequestedState state = this.deferredOpsQueue.get(dataInfo);
            if (state == null) {
                if (deferredOp == DeferredOp.WRITE) {
                    requestWrite(dataInfo);
                } else if (deferredOp == DeferredOp.ARCHIVE) {
                    deferredOpsQueue.put(dataInfo, RequestedState.ARCHIVE_REQUESTED);
                } else if (deferredOp == DeferredOp.RESTORE) {
                    deferredOpsQueue.put(dataInfo, RequestedState.RESTORE_REQUESTED);
                }
            } else if (state == RequestedState.ARCHIVE_REQUESTED) {
                if (deferredOp == DeferredOp.WRITE) {
                    requestWrite(dataInfo);
                    deferredOpsQueue.put(dataInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
                } else if (deferredOp == DeferredOp.RESTORE) {
                    deferredOpsQueue.put(dataInfo, RequestedState.RESTORE_REQUESTED);
                }
            } else if (state == RequestedState.RESTORE_REQUESTED) {
                if (deferredOp == DeferredOp.WRITE) {
                    requestWrite(dataInfo);
                } else if (deferredOp == DeferredOp.ARCHIVE) {
                    deferredOpsQueue.put(dataInfo, RequestedState.ARCHIVE_REQUESTED);
                }
            } else if (state == RequestedState.WRITE_REQUESTED) {
                if (deferredOp == DeferredOp.WRITE) {
                    setDelay(dataInfo);
                } else if (deferredOp == DeferredOp.ARCHIVE) {
                    deferredOpsQueue.put(dataInfo, RequestedState.WRITE_THEN_ARCHIVE_REQUESTED);
                }
            } else if (state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
                if (deferredOp == DeferredOp.WRITE) {
                    setDelay(dataInfo);
                } else if (deferredOp == DeferredOp.RESTORE) {
                    deferredOpsQueue.put(dataInfo, RequestedState.WRITE_REQUESTED);
                }
            }
        }

    }


    private void requestWrite(DataInfoBase dsInfo) throws InternalException {
        try {
            Path marker = markerDir.resolve(Long.toString(dsInfo.getId()));
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


    private void setDelay(DataInfoBase dsInfo) {
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
                synchronized (deferredOpsQueue) {
                    final long now = System.currentTimeMillis();
                    Map<DataInfoBase, RequestedState> newOps = new HashMap<>();
                    final Iterator<Entry<DataInfoBase, RequestedState>> it = deferredOpsQueue.entrySet().iterator();
                    while (it.hasNext()) {
                        final Entry<DataInfoBase, RequestedState> opEntry = it.next();
                        final var dsInfo = (DataSetInfo) opEntry.getKey();


                        if(dsInfo == null) throw new RuntimeException("Could not cast DataInfoBase to DataSetInfo. Did you handed over another sub type?");

                        if (!dataInfoChanging.containsKey(dsInfo)) {
                            final RequestedState state = opEntry.getValue();
                            if (state == RequestedState.WRITE_REQUESTED
                                    || state == RequestedState.WRITE_THEN_ARCHIVE_REQUESTED) {
                                if (now > writeTimes.get(dsInfo)) {
                                    try {
                                        Lock lock = lockManager.lock(dsInfo, LockType.SHARED);
                                        logger.debug("Will process " + dsInfo + " with " + state);
                                        writeTimes.remove(dsInfo);
                                        dataInfoChanging.put(dsInfo, RequestedState.WRITE_REQUESTED);
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
                                    dataInfoChanging.put(dsInfo, state);
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
                                    dataInfoChanging.put(dsInfo, state);
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
                    deferredOpsQueue.putAll(newOps);
                }

            } finally {
                timer.schedule(new DsProcessQueue(), processQueueIntervalMillis);
            }

        }

    }

}