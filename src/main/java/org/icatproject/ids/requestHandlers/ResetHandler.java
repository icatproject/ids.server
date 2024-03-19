package org.icatproject.ids.requestHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionFactory;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class ResetHandler extends RequestHandlerBase {

    public ResetHandler() {
        super(PreparedDataStatus.NOMATTER, RequestType.RESET);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        String preparedId = parameters.get("preparedId").getString();
        String sessionId = parameters.get("sessionId").getString();
        String investigationIds = parameters.get("investigationIds").getString();
        String datasetIds = parameters.get("datasetIds").getString();
        String datafileIds = parameters.get("datafileIds").getString();
        String ip = parameters.get("ip").getString();
        
        if (preparedId != null) {
            this.reset(preparedId, ip);
        } else {
            this.reset(sessionId, investigationIds, datasetIds, datafileIds, ip);
        }

        return ValueContainer.getVoid();
    }


    private void reset(String preparedId, String ip) throws BadRequestException, InternalException, NotFoundException {
        long start = System.currentTimeMillis();

        logger.info(String.format("New webservice request: reset preparedId=%s", preparedId));
        var serviceProvider = ServiceProvider.getInstance();

        // Validate
        validateUUID("preparedId", preparedId);

        // Do it
        Prepared preparedJson;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            preparedJson = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        var dataSelection = DataSelectionFactory.get(preparedJson.dsInfos, preparedJson.dfInfos, preparedJson.emptyDatasets, preparedJson.fileLength, this.getRequestType());

        this.recordSuccess(dataSelection, serviceProvider.getFsm());

        if (serviceProvider.getLogSet().contains(CallType.MIGRATE)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            serviceProvider.getTransmitter().processMessage("reset", ip, body, start);
        }
    }


    private void reset(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {

        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        // Log and validate
        logger.info("New webservice request: reset " + "investigationIds='" + investigationIds + "' " + "datasetIds='"
                + datasetIds + "' " + "datafileIds='" + datafileIds + "'");

        validateUUID("sessionId", sessionId);

        final DataSelectionBase dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);

        // Do it
        this.recordSuccess(dataSelection, serviceProvider.getFsm());

        if (serviceProvider.getLogSet().contains(CallType.MIGRATE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("reset", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }
    }
    

    private void recordSuccess(DataSelectionBase dataSelection, FiniteStateMachine fsm) {
        for (DataInfoBase dataInfo : dataSelection.getPrimaryDataInfos().values()) {
            fsm.recordSuccess(dataInfo.getId());
        }
    }

    
}