package org.icatproject.ids.finiteStateMachine;

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
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager;
import org.icatproject.ids.services.PropertyHandler;
import org.icatproject.ids.services.LockManager.LockInfo;

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

    public abstract void queue(DataInfoBase dataInfo, DeferredOp deferredOp) throws InternalException;
    protected abstract void scheduleTimer();
    protected abstract void addDataInfoJson(JsonGenerator gen);

    public enum RequestedState {
        ARCHIVE_REQUESTED, DELETE_REQUESTED, RESTORE_REQUESTED, WRITE_REQUESTED, WRITE_THEN_ARCHIVE_REQUESTED
    }

    protected static Logger logger = LoggerFactory.getLogger(FiniteStateMachine.class);

    /*
     * Note that the veriable processOpsDelayMillis is used to either delay all deferred
     * datafile operations or to delay dataset writes, depending on the setting of storageUnit.
     */
    protected long processOpsDelayMillis;

    protected Map<DataInfoBase, RequestedState> deferredOpsQueue = new HashMap<>();

    protected Map<DataInfoBase, RequestedState> dataInfoChanging = new HashMap<>();

    protected Path markerDir;
    protected long processQueueIntervalMillis;

    protected PropertyHandler propertyHandler;

    protected IcatReader reader;

    protected LockManager lockManager;

    protected Timer timer = new Timer("FSM Timer");

    protected Long processOpsTime;

    protected Set<Long> failures = ConcurrentHashMap.newKeySet();

    public void exit() {
        timer.cancel();
        logger.info("Cancelled timer");
    }

    /**
     * Find any DataFileInfo which may be offline
     */
    public Set<DataInfoBase> getMaybeOffline() {
        Map<DataInfoBase, RequestedState> union;
        synchronized (deferredOpsQueue) {
            union = new HashMap<>(dataInfoChanging);
            union.putAll(deferredOpsQueue);
        }
        Set<DataInfoBase> result = new HashSet<>();
        for (Entry<DataInfoBase, RequestedState> entry : union.entrySet()) {
            if (entry.getValue() != RequestedState.WRITE_REQUESTED) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Find any DataFileInfo which are being restored or are queued for restoration
     */
    public Set<DataInfoBase> getRestoring() {
        Map<DataInfoBase, RequestedState> union;
        synchronized (deferredOpsQueue) {
            union = new HashMap<>(dataInfoChanging);
            union.putAll(deferredOpsQueue);
        }
        Set<DataInfoBase> result = new HashSet<>();
        for (Entry<DataInfoBase, RequestedState> entry : union.entrySet()) {
            if (entry.getValue() == RequestedState.RESTORE_REQUESTED) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    protected void addDataInfoJsonFromDeferredOpsQueue(JsonGenerator gen) {
        Map<DataInfoBase, RequestedState> union;
        synchronized (deferredOpsQueue) {
            union = new HashMap<>(dataInfoChanging);
            union.putAll(deferredOpsQueue);
        }
        gen.writeStartArray("opsQueue");
        for (Entry<DataInfoBase, RequestedState> entry : union.entrySet()) {
            DataInfoBase item = entry.getKey();
            gen.writeStartObject().write("data", item.toString()).write("request", entry.getValue().name())
                    .writeEnd();
        }
        gen.writeEnd(); // end Array("opsQueue")
    }

    public String getServiceStatus() throws InternalException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            
            this.addDataInfoJson(gen);

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


    public void init() {

        try {
            synchronized (inited) {
                if(!inited) {
                    propertyHandler = PropertyHandler.getInstance();
                    processQueueIntervalMillis = propertyHandler.getProcessQueueIntervalSeconds() * 1000L;

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

    public void removeFromChanging(DataInfoBase dfInfo) {
        synchronized (deferredOpsQueue) {
            dataInfoChanging.remove(dfInfo);
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
