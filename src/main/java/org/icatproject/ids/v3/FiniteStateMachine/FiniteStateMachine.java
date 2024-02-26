package org.icatproject.ids.v3.FiniteStateMachine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.IcatReader;
import org.icatproject.ids.LockManager;
import org.icatproject.ids.PropertyHandler;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.LockManager.LockInfo;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.DataSetInfo;

public abstract class FiniteStateMachine {


    private static FiniteStateMachine instance;

    private static Boolean inited = false;

    protected FiniteStateMachine(IcatReader reader, LockManager lockManager) {
        this.reader = reader;
        this.lockManager = lockManager;
    }

    public static void createInstance(IcatReader reader, LockManager lockManager, StorageUnit storageUnit) {

        if(instance == null) {
            if(storageUnit == StorageUnit.DATAFILE)
                instance = new FiniteStateMachineForStorageUnitDatafile(reader, lockManager);
            else if(storageUnit == StorageUnit.DATASET)
                instance = new FiniteStateMachineForStorageUnitDataset(reader, lockManager);
            else
                instance = new FiniteStateMachineForSingleLevelStorage(reader, lockManager);
        }
    }

    public static FiniteStateMachine getInstance() {
        if(instance != null) {
            return instance;
        }

        // If this assert was executed: Instance of FiniteStateMachine is not created. At First createInstance() has to be called at least once.
        throw new RuntimeException("Instance of FiniteStateMachine is not created. At First createInstance() has to be called at least once.");
    }

    public enum RequestedState {
        ARCHIVE_REQUESTED, DELETE_REQUESTED, RESTORE_REQUESTED, WRITE_REQUESTED, WRITE_THEN_ARCHIVE_REQUESTED
    }

    protected static Logger logger = LoggerFactory.getLogger(FiniteStateMachine.class);

    /*
     * Note that the veriable processOpsDelayMillis is used to either delay all deferred
     * datafile operations or to delay dataset writes, depending on the setting of storageUnit.
     */
    protected long processOpsDelayMillis;

    protected Map<DataFileInfo, RequestedState> deferredDfOpsQueue = new HashMap<>();

    protected Map<DataSetInfo, RequestedState> deferredDsOpsQueue = new HashMap<>();

    protected Map<DataFileInfo, RequestedState> dfChanging = new HashMap<>();

    protected Map<DataSetInfo, RequestedState> dsChanging = new HashMap<>();

    protected Path markerDir;
    protected long processQueueIntervalMillis;

    protected PropertyHandler propertyHandler;

    protected IcatReader reader;


    protected LockManager lockManager;

    protected StorageUnit storageUnit;

    protected Timer timer = new Timer("FSM Timer");

    protected Long processOpsTime;

    protected Map<DsInfo, Long> writeTimes = new HashMap<>();

    protected Set<Long> failures = ConcurrentHashMap.newKeySet();

    public void exit() {
        timer.cancel();
        logger.info("Cancelled timer");
    }

    /**
     * Find any DataFileInfo which may be offline
     */
    public Set<DataFileInfo> getDfMaybeOffline() {
        Map<DataFileInfo, RequestedState> union;
        synchronized (deferredDfOpsQueue) {
            union = new HashMap<>(dfChanging);
            union.putAll(deferredDfOpsQueue);
        }
        Set<DataFileInfo> result = new HashSet<>();
        for (Entry<DataFileInfo, RequestedState> entry : union.entrySet()) {
            if (entry.getValue() != RequestedState.WRITE_REQUESTED) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Find any DataFileInfo which are being restored or are queued for restoration
     */
    public Set<DataFileInfo> getDfRestoring() {
        Map<DataFileInfo, RequestedState> union;
        synchronized (deferredDfOpsQueue) {
            union = new HashMap<>(dfChanging);
            union.putAll(deferredDfOpsQueue);
        }
        Set<DataFileInfo> result = new HashSet<>();
        for (Entry<DataFileInfo, RequestedState> entry : union.entrySet()) {
            if (entry.getValue() == RequestedState.RESTORE_REQUESTED) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Find any DataSetInfo which may be offline
     */
    public Set<DataSetInfo> getDsMaybeOffline() {
        Map<DataSetInfo, RequestedState> union;
        synchronized (deferredDsOpsQueue) {
            union = new HashMap<>(dsChanging);
            union.putAll(deferredDsOpsQueue);
        }
        Set<DataSetInfo> result = new HashSet<>();
        for (Entry<DataSetInfo, RequestedState> entry : union.entrySet()) {
            if (entry.getValue() != RequestedState.WRITE_REQUESTED) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Find any DsInfo which are being restored or are queued for restoration
     */
    public Set<DataSetInfo> getDsRestoring() {
        Map<DataSetInfo, RequestedState> union;
        synchronized (deferredDsOpsQueue) {
            union = new HashMap<>(dsChanging);
            union.putAll(deferredDsOpsQueue);
        }
        Set<DataSetInfo> result = new HashSet<>();
        for (Entry<DataSetInfo, RequestedState> entry : union.entrySet()) {
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

    public abstract void queue(DataInfoBase dataInfo, DeferredOp deferredOp) throws InternalException;
    protected abstract void scheduleTimer();

    public void init() {

        try {
            synchronized (inited) {
                if(!inited) {
                    propertyHandler = PropertyHandler.getInstance();
                    processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;
                    storageUnit = propertyHandler.getStorageUnit();

                    this.scheduleTimer();
                        
                    markerDir = propertyHandler.getCacheDir().resolve("marker");
                    Files.createDirectories(markerDir);
                    inited = true;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("FiniteStateMachine reports " + e.getClass() + " " + e.getMessage());
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

    public void checkFailure(Long id) throws InternalException {
        if (failures.contains(id)) {
            throw new InternalException("Restore failed");
        }
    }

}
