package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.v3.DataSelectionV3Base;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.enums.CallType;
import org.icatproject.ids.v3.enums.DeferredOp;
import org.icatproject.ids.v3.enums.RequestType;
import org.icatproject.ids.v3.models.ValueContainer;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class RestoreHandler extends RequestHandlerBase {

    public RestoreHandler() {
        super(new StorageUnit[]{StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.RESTORE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        var sessionId = parameters.get("sessionId").getString();
        var investigationIds = parameters.get("investigationIds").getString();
        var datasetIds = parameters.get("datasetIds").getString();
        var datafileIds = parameters.get("datafileIds").getString();
        var ip = parameters.get("ip").getString();

        // Log and validate
        logger.info("New webservice request: restore " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID("sessionId", sessionId);

        // Do it
        DataSelectionV3Base dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);
        dataSelection.scheduleTasks(DeferredOp.RESTORE);

        if (serviceProvider.getLogSet().contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                serviceProvider.getTransmitter().processMessage("restore", ip, baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return ValueContainer.getVoid();
    }
}