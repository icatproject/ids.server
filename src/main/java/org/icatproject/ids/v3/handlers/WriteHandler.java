package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.LockManager.LockType;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class WriteHandler extends RequestHandlerBase {

    public WriteHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.WRITE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        String sessionId = parameters.get("sessionId").getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        String ip = parameters.get("ip").getString();

        // Log and validate
        logger.info("New webservice request: write " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        if (!serviceProvider.getPropertyHandler().getEnableWrite()) {
            throw new NotImplementedException("This operation has been configured to be unavailable");
        }

        validateUUID("sessionId", sessionId);

        final DataSelectionBase dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);

        // Do it
        Map<Long, DataInfoBase> dsInfos = dataSelection.getDsInfo();

        try (Lock lock = serviceProvider.getLockManager().lock(dsInfos.values(), LockType.SHARED)) {
            if (twoLevel) {
                boolean maybeOffline = false;
                for (DataInfoBase dataInfo : dataSelection.getPrimaryDataInfos().values()) {
                    if (!dataSelection.existsInMainStorage(dataInfo)) {
                        maybeOffline = true;
                    }
                }
                if (maybeOffline) {
                    throw new DataNotOnlineException("Requested data is not online, write request refused");
                }
            }

            logger.info("### PreScheduleTask - StorageUnit: " + storageUnit);
            dataSelection.scheduleTasks(DeferredOp.WRITE);
            logger.info("### PostScheduleTask");

        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, write failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            logger.error("I/O error " + e.getMessage() + " writing");
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        if (serviceProvider.getLogSet().contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                serviceProvider.getTransmitter().processMessage("write", ip, baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return ValueContainer.getVoid();
    }
}