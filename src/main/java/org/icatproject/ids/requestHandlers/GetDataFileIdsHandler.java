package org.icatproject.ids.requestHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
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
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.v3.RequestHandlerBase;
import org.icatproject.ids.v3.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetDataFileIdsHandler extends RequestHandlerBase {

    public GetDataFileIdsHandler() {
        super(new StorageUnit[] {StorageUnit.DATAFILE, StorageUnit.DATASET, null}, RequestType.GETDATAFILEIDS);
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
            return new ValueContainer(this.getDatafileIds(preparedId, ip));
        } else {
            return new ValueContainer(this.getDatafileIds(sessionId, investigationIds, datasetIds, datafileIds, ip));
        }
    }


    private String getDatafileIds(String preparedId, String ip)
            throws BadRequestException, InternalException, NotFoundException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info("New webservice request: getDatafileIds preparedId = '" + preparedId);

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

        final boolean zip = prepared.zip;
        final boolean compress = prepared.compress;
        final SortedMap<Long, DataInfoBase> dfInfos = prepared.dfInfos;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.write("zip", zip);
            gen.write("compress", compress);
            gen.writeStartArray("ids");
            for (DataInfoBase dfInfo : dfInfos.values()) {
                gen.write(dfInfo.getId());
            }
            gen.writeEnd().writeEnd().close();
        }
        String resp = baos.toString();

        var serviceProvider = ServiceProvider.getInstance();

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("preparedId", preparedId);
                gen.writeEnd();
            }
            serviceProvider.getTransmitter().processMessage("getDatafileIds", ip, baos.toString(), start);
        }

        return resp;
    }

    private String getDatafileIds(String sessionId, String investigationIds, String datasetIds, String datafileIds,
                                 String ip)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, NotImplementedException {

        long start = System.currentTimeMillis();

        // Log and validate
        logger.info(String.format(
                "New webservice request: getDatafileIds investigationIds=%s, datasetIds=%s, datafileIds=%s",
                investigationIds, datasetIds, datafileIds));

        validateUUID("sessionId", sessionId);

        final DataSelectionBase dataSelection = this.getDataSelection(sessionId, investigationIds, datasetIds, datafileIds);

        // Do it
        Map<Long, DataInfoBase> dfInfos = dataSelection.getDfInfo();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.writeStartArray("ids");
            for (DataInfoBase dfInfo : dfInfos.values()) {
                gen.write(dfInfo.getId());
            }
            gen.writeEnd().writeEnd().close();
        }
        String resp = baos.toString();

        var serviceProvider = ServiceProvider.getInstance();

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            baos = new ByteArrayOutputStream();
            try {
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.writeEnd();
                }
                serviceProvider.getTransmitter().processMessage("getDatafileIds", ip, baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return resp;

    }

}