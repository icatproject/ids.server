package org.icatproject.ids.requestHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class RestoreHandler extends RequestHandlerBase {

    public RestoreHandler() {
        super(PreparedDataStatus.NOMATTER, RequestType.RESTORE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        var sessionId = parameters.get(RequestIdNames.sessionId).getString();
        var investigationIds = parameters.get("investigationIds").getString();
        var datasetIds = parameters.get("datasetIds").getString();
        var datafileIds = parameters.get("datafileIds").getString();
        var ip = parameters.get("ip").getString();

        // Log and validate
        logger.info("New webservice request: restore " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID(RequestIdNames.sessionId, sessionId);

        // Do it
        DataSelectionBase dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);
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