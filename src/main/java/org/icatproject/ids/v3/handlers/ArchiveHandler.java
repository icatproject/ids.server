package org.icatproject.ids.v3.handlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
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
import org.icatproject.ids.v3.DataSelectionV3Base;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.models.ValueContainer;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class ArchiveHandler extends RequestHandlerBase {

    public ArchiveHandler() {
        super(new StorageUnit[] {StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.ARCHIVE);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters) throws BadRequestException,
            InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();

        String sessionId = parameters.get("sessionId").getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        String ip = parameters.get("ip").getString();   

        // Log and validate
        logger.info("New webservice request: archive " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID("sessionId", sessionId);
        
        // Do it
        DataSelectionV3Base dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);
        dataSelection.scheduleTasks(DeferredOp.ARCHIVE);

        if (ServiceProvider.getInstance().getLogSet().contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", ServiceProvider.getInstance().getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                ServiceProvider.getInstance().getTransmitter().processMessage("archive", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return ValueContainer.getVoid();

    }

}