package org.icatproject.ids.requestHandlers.getStatusHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.OperationIdTypes;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetStatusHandlerForUnpreparedData extends GetStatusHandler {

    public GetStatusHandlerForUnpreparedData() {
        super(new OperationIdTypes[]{OperationIdTypes.SESSIONID, OperationIdTypes.ANONYMOUS});
    }

    @Override
    protected DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters)
            throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {

        String sessionId = parameters.get(RequestIdNames.sessionId).getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        
        // Log and validate
        logger.info(
                String.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
                        investigationIds, datasetIds, datafileIds));

        if (sessionId == null) {
            try {
                sessionId = ServiceProvider.getInstance().getIcatReader().getSessionId();
            } catch (IcatException_Exception e) {
                throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
            }
        } else {
            validateUUID(RequestIdNames.sessionId, sessionId);
        }

        return this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    protected ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters)
            throws InternalException, BadRequestException, IcatException_Exception {

        String sessionId = parameters.get(RequestIdNames.sessionId).getString();
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            if (sessionId != null) {
                gen.write("userName", ServiceProvider.getInstance().getIcat().getUserName(sessionId));
            }
            addIds(gen, parameters.get("investigationIds").getString(), parameters.get("datasetIds").getString(), parameters.get("datafileIds").getString());
            gen.writeEnd();
        }

        return baos;
    }
    
}