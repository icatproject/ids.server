package org.icatproject.ids.requestHandlers.getDataHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.PreparedDataStatus;
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

public class GetDataHandlerForUnpreparedData extends GetDataHandler {

    public GetDataHandlerForUnpreparedData() {
        super(PreparedDataStatus.UNPREPARED);
    }

    @Override
    protected DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters, long offset)
            throws BadRequestException, InternalException, NotFoundException, InsufficientPrivilegesException, NotImplementedException {

            String sessionId = parameters.getOrDefault(RequestIdNames.sessionId, ValueContainer.getInvalid()).getString(); 
            String investigationIds = parameters.getOrDefault("investigationIds", ValueContainer.getInvalid()).getString();
            String datasetIds = parameters.getOrDefault("datasetIds", ValueContainer.getInvalid()).getString();
            String datafileIds = parameters.getOrDefault("datafileIds", ValueContainer.getInvalid()).getString();

        // Log and validate
        logger.info(String.format("New webservice request: getData investigationIds=%s, datasetIds=%s, datafileIds=%s",
                investigationIds, datasetIds, datafileIds));

        validateUUID(RequestIdNames.sessionId, sessionId);

        return this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    protected ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters,
            Long transferId) throws InternalException, IcatException_Exception, BadRequestException {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.write("transferId", transferId);
            gen.write("userName", ServiceProvider.getInstance().getIcat().getUserName(parameters.get(RequestIdNames.sessionId).getString()));
            addIds( gen, 
                    parameters.getOrDefault("investigationIds", ValueContainer.getInvalid()).getString(), 
                    parameters.getOrDefault("datasetIds", ValueContainer.getInvalid()).getString(), 
                    parameters.getOrDefault("datafileIds", ValueContainer.getInvalid()).getString()
            );
            gen.writeEnd();
        }

        return baos;
    }

    @Override
    protected boolean mustZip(boolean zip, DataSelectionBase dataSelection) {
        return zip ? true : dataSelection.mustZip();
    }
}