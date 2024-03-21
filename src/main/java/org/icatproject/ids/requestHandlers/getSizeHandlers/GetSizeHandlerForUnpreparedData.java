package org.icatproject.ids.requestHandlers.getSizeHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;

import org.icatproject.Datafile;
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
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

public class GetSizeHandlerForUnpreparedData extends GetSizeHandler {

    public GetSizeHandlerForUnpreparedData() {
        super(OperationIdTypes.SESSIONID);
    }

    @Override
    public long getSize(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {
        
        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        String sessionId = parameters.get(RequestIdNames.sessionId).getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        String ip = parameters.get("ip").getString();

        // Log and validate
        logger.info(String.format("New webservice request: getSize investigationIds=%s, datasetIds=%s, datafileIds=%s",
                investigationIds, datasetIds, datafileIds));

        validateUUID(RequestIdNames.sessionId, sessionId);

        List<Long> dfids = DataSelectionBase.getValidIds("datafileIds", datafileIds);
        List<Long> dsids = DataSelectionBase.getValidIds("datasetIds", datasetIds);
        List<Long> invids = DataSelectionBase.getValidIds("investigationIds", investigationIds);

        long size = 0;
        if (dfids.size() + dsids.size() + invids.size() == 1) {
            size = getSizeFor(sessionId, invids, "df.dataset.investigation.id")
                    + getSizeFor(sessionId, dsids, "df.dataset.id") + getSizeFor(sessionId, dfids, "df.id");
            logger.debug("Fast computation for simple case");
            if (size == 0) {
                try {
                    if (dfids.size() != 0) {
                        Datafile datafile = (Datafile) serviceProvider.getIcat().get(sessionId, "Datafile", dfids.get(0));
                        if (datafile.getLocation() == null) {
                            throw new NotFoundException("Datafile not found");
                        }
                    }
                    if (dsids.size() != 0) {
                        serviceProvider.getIcat().get(sessionId, "Dataset", dsids.get(0));
                    }
                    if (invids.size() != 0) {
                        serviceProvider.getIcat().get(sessionId, "Investigation", invids.get(0));
                    }
                } catch (IcatException_Exception e) {
                    throw new NotFoundException(e.getMessage());
                }
            }
        } else {
            logger.debug("Slow computation for normal case");
            final DataSelectionBase dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);

            size = this.updateSizeFromDataInfoIds(size, dataSelection.getDfInfo(), sessionId);
        }

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("getSize", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return size;
    }
    
}