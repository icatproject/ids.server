package org.icatproject.ids.requestHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Set;
import java.util.SortedMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.enums.Status;
import org.icatproject.ids.enums.StorageUnit;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetStatusHandler extends RequestHandlerBase {

    public GetStatusHandler() {
        super(new StorageUnit[] {StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.GETSTATUS );
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
            return new ValueContainer(this.getStatus(preparedId, ip));
        } else {
            return new ValueContainer(this.getStatus(sessionId, investigationIds, datasetIds, datafileIds, ip));
        }
    }


    private String getStatus(String preparedId, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        // Log and validate
        logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
        validateUUID("preparedId", preparedId);

        // Do it
        Prepared prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        final SortedMap<Long, DataInfoBase> dfInfos = prepared.dfInfos;
        final SortedMap<Long, DataInfoBase> dsInfos = prepared.dsInfos;
        Set<Long> emptyDatasets = prepared.emptyDatasets;

        // Do it
        var dataSelection = this.getDataSelection(dsInfos, dfInfos, emptyDatasets, prepared.fileLength);

        Status status = this.getStatus(dataSelection);

        logger.debug("Status is " + status.name());

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            serviceProvider.getTransmitter().processMessage("getStatus", ip, body, start);
        }

        return status.name();

    }


    private String getStatus(String sessionId, String investigationIds, String datasetIds, String datafileIds, String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {

        long start = System.currentTimeMillis();
        var serviceProvider = ServiceProvider.getInstance();

        // Log and validate
        logger.info(
                String.format("New webservice request: getStatus investigationIds=%s, datasetIds=%s, datafileIds=%s",
                        investigationIds, datasetIds, datafileIds));

        if (sessionId == null) {
            try {
                sessionId = serviceProvider.getIcatReader().getSessionId();
            } catch (IcatException_Exception e) {
                throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
            }
        } else {
            validateUUID("sessionId", sessionId);
        }

        // Do it
        DataSelectionBase dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);
        Status status = this.getStatus(dataSelection);


        logger.debug("Status is " + status.name());

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    if (sessionId != null) {
                        gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    }
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("getStatus", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return status.name();

    }


    private Status getStatus(DataSelectionBase dataSelection) throws InternalException {
        Status status = Status.ONLINE;
        var serviceProvider = ServiceProvider.getInstance();

        Set<DataInfoBase> restoring = serviceProvider.getFsm().getRestoring();
        Set<DataInfoBase> maybeOffline = serviceProvider.getFsm().getMaybeOffline();
        for (DataInfoBase dataInfo : dataSelection.getPrimaryDataInfos().values()) {
            serviceProvider.getFsm().checkFailure(dataInfo.getId());
            if (restoring.contains(dataInfo)) {
                status = Status.RESTORING;
            } else if (maybeOffline.contains(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            } else if (!dataSelection.existsInMainStorage(dataInfo)) {
                status = Status.ARCHIVED;
                break;
            }
        }

        return status;
    }
}