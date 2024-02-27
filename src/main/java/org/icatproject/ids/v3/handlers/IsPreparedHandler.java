package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.DataSelectionV3Base;
import org.icatproject.ids.v3.PreparedV3;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.DataInfoBase;
import org.icatproject.ids.v3.models.ValueContainer;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class IsPreparedHandler extends RequestHandlerBase {

    public IsPreparedHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.ISPREPARED);
    }

    class PreparedStatus {
        public ReentrantLock lock = new ReentrantLock();
        public Long fromDfElement;
        public Future<?> future;
        public Long fromDsElement;
    }

    private Map<String, PreparedStatus> preparedStatusMap = new ConcurrentHashMap<>();

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();

        String preparedId = parameters.get("preparedId").getString();
        String ip = parameters.get("ip").getString();

        logger.info(String.format("New webservice request: isPrepared preparedId=%s", preparedId));

        // Validate
        validateUUID("preparedId", preparedId);

        // Do it
        boolean prepared = true;

        PreparedV3 preparedJson;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            preparedJson = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        PreparedStatus status = preparedStatusMap.computeIfAbsent(preparedId, k -> new PreparedStatus());

        if (!status.lock.tryLock()) {
            logger.debug("Lock held for evaluation of isPrepared for preparedId {}", preparedId);
            return new ValueContainer(false);
        }
        try {
            Future<?> future = status.future;
            if (future != null) {
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        throw new InternalException(e.getClass() + " " + e.getMessage());
                    } catch (InterruptedException e) {
                        // Ignore
                    } finally {
                        status.future = null;
                    }
                } else {
                    logger.debug("Background process still running for preparedId {}", preparedId);
                    return new ValueContainer(false);
                }
            }

            var serviceProvider = ServiceProvider.getInstance();
            DataSelectionV3Base dataSelection = this.getDataSelection(preparedJson.dsInfos, preparedJson.dfInfos, preparedJson.emptyDatasets);

            if (storageUnit == StorageUnit.DATASET) {
                Collection<DataInfoBase> toCheck = status.fromDsElement == null ? dataSelection.getPrimaryDataInfos()
                        : preparedJson.dsInfos.tailMap(status.fromDsElement).values();
                logger.debug("Will check online status of {} entries", toCheck.size());
                for (DataInfoBase dsInfo : toCheck) {
                    serviceProvider.getFsm().checkFailure(dsInfo.getId());
                    if (dataSelection.restoreIfOffline(dsInfo)) {
                        prepared = false;
                        status.fromDsElement = dsInfo.getId();
                        toCheck = preparedJson.dsInfos.tailMap(status.fromDsElement).values();
                        logger.debug("Will check in background status of {} entries", toCheck.size());
                        status.future = threadPool.submit(new RunPrepDataInfoCheck(toCheck, dataSelection));
                        break;
                    }
                }
                if (prepared) {
                    toCheck = status.fromDsElement == null ? Collections.emptySet()
                            : preparedJson.dsInfos.headMap(status.fromDsElement).values();
                    logger.debug("Will check finally online status of {} entries", toCheck.size());
                    for (DataInfoBase dsInfo : toCheck) {
                        serviceProvider.getFsm().checkFailure(dsInfo.getId());
                        if (dataSelection.restoreIfOffline(dsInfo)) {
                            prepared = false;
                        }
                    }
                }
            } else if (storageUnit == StorageUnit.DATAFILE) {
                SortedMap<Long, DataInfoBase> toCheck = status.fromDfElement == null ? preparedJson.dfInfos
                        : preparedJson.dfInfos.tailMap(status.fromDfElement);
                logger.debug("Will check online status of {} entries", toCheck.size());
                for (DataInfoBase dfInfo : toCheck.values()) {
                    serviceProvider.getFsm().checkFailure(dfInfo.getId());
                    if (dataSelection.restoreIfOffline(dfInfo)) {
                        prepared = false;
                        status.fromDfElement = dfInfo.getId();
                        toCheck = preparedJson.dfInfos.tailMap(status.fromDfElement);
                        logger.debug("Will check in background status of {} entries", toCheck.size());
                        status.future = threadPool.submit(new RunPrepDataInfoCheck(toCheck.values(), dataSelection));
                        break;
                    }
                }
                if (prepared) {
                    toCheck = status.fromDfElement == null ? new TreeMap<>()
                            : preparedJson.dfInfos.headMap(status.fromDfElement);
                    logger.debug("Will check finally online status of {} entries", toCheck.size());
                    for (DataInfoBase dfInfo : toCheck.values()) {
                        serviceProvider.getFsm().checkFailure(dfInfo.getId());
                        if (dataSelection.restoreIfOffline(dfInfo)) {
                            prepared = false;
                        }
                    }
                }
            }

            if (serviceProvider.getLogSet().contains(CallType.INFO)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("preparedId", preparedId);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("isPrepared", ip, body, start);
            }

            return new ValueContainer(prepared);

        } finally {
            status.lock.unlock();
        }
    }

    public class RunPrepDataInfoCheck implements Callable<Void> {

        private Collection<DataInfoBase> toCheck;
        private DataSelectionV3Base dataselection;

        public RunPrepDataInfoCheck(Collection<DataInfoBase> toCheck, DataSelectionV3Base dataSelection) {
            this.toCheck = toCheck;
            this.dataselection = dataSelection;
        }

        @Override
        public Void call() throws Exception {
            for(DataInfoBase dataInfo : toCheck) {
                ServiceProvider.getInstance().getFsm().checkFailure(dataInfo.getId());
                dataselection.restoreIfOffline(dataInfo);
            }
            return null;
        }
        
    }
}