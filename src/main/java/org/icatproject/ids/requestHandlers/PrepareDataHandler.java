package org.icatproject.ids.requestHandlers;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.icatproject.IcatException_Exception;
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
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class PrepareDataHandler extends RequestHandlerBase {

    public PrepareDataHandler() {
        super(PreparedDataStatus.NOMATTER, RequestType.PREPAREDATA);
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
        boolean compress = parameters.get("compress").getBool();
        boolean zip = parameters.get("zip").getBool();
        String ip = parameters.get("ip").getString();

        // Log and validate
        logger.info("New webservice request: prepareData " + "investigationIds='" + investigationIds + "' "
                + "datasetIds='" + datasetIds + "' " + "datafileIds='" + datafileIds + "' " + "compress='" + compress
                + "' " + "zip='" + zip + "'");

        validateUUID("sessionId", sessionId);

        final DataSelectionBase dataSelection = this.getDataSelection(sessionId,
                investigationIds, datasetIds, datafileIds);

        // Do it
        String preparedId = UUID.randomUUID().toString();

        Map<Long, DataInfoBase> dsInfos = dataSelection.getDsInfo();
        Set<Long> emptyDs = dataSelection.getEmptyDatasets();
        Map<Long, DataInfoBase> dfInfos = dataSelection.getDfInfo();
        
        dataSelection.restoreDataInfos();

        if (dataSelection.mustZip()) {
            zip = true;
        }

        logger.debug("Writing to " + preparedDir.resolve(preparedId));
        try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(preparedDir.resolve(preparedId)))) {
            pack(stream, zip, compress, dsInfos, dfInfos, emptyDs, dataSelection.getFileLength().isEmpty() ? 0 : dataSelection.getFileLength().getAsLong());
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        logger.debug("preparedId is " + preparedId);

        if (serviceProvider.getLogSet().contains(CallType.PREPARE)) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("userName", serviceProvider.getIcat().getUserName(sessionId));
                    addIds(gen, investigationIds, datasetIds, datafileIds);
                    gen.write("preparedId", preparedId);
                    gen.writeEnd();
                }
                String body = baos.toString();
                serviceProvider.getTransmitter().processMessage("prepareData", ip, body, start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return new ValueContainer(preparedId);
    }
}