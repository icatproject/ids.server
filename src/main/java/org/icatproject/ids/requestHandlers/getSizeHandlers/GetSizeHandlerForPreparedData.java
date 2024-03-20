package org.icatproject.ids.requestHandlers.getSizeHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetSizeHandlerForPreparedData extends GetSizeHandler {
    
    public GetSizeHandlerForPreparedData() {
        super(PreparedDataStatus.PREPARED);
    }

    @Override
    public long getSize(HashMap<String, ValueContainer> parameters) throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {

        long start = System.currentTimeMillis();

        var serviceProvider = ServiceProvider.getInstance();

        String preparedId = parameters.get(RequestIdNames.preparedId).getString();

        // Log and validate
        logger.info("New webservice request: getSize preparedId = '{}'", preparedId);
        validateUUID(RequestIdNames.preparedId, preparedId);

        // Do it
        Prepared prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        final Map<Long, DataInfoBase> dfInfos = prepared.dfInfos;

        // Note that the "fast computation for the simple case" (see the other getSize() implementation) is not
        // available when calling getSize() with a preparedId.
        logger.debug("Slow computation for normal case");
        String sessionId;
        try {
            sessionId = serviceProvider.getIcatReader().getSessionId();
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
        }
        long size = 0;

        size = this.updateSizeFromDataInfoIds(size, dfInfos, sessionId);

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write(RequestIdNames.preparedId, preparedId);
                gen.writeEnd();
            }
            String body = baos.toString();
            serviceProvider.getTransmitter().processMessage("getSize", parameters.get("ip").getString(), body, start);
        }

        return size;
    }
}