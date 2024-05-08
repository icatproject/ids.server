package org.icatproject.ids.requestHandlers;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;

import jakarta.json.stream.JsonGenerator;

public class PrepareDataHandler extends DataRequestHandler {

    Boolean compress;
    Boolean zip;
    String preparedId;

    public PrepareDataHandler(String ip, String sessionId, String investigationIds, String datasetIds, String datafileIds, Boolean compress, Boolean zip) {
        super(RequestType.PREPAREDATA,ip, sessionId, investigationIds, datasetIds, datafileIds);

        this.zip = zip;
        this.compress = compress;
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionBase dataSelection)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException { 

        preparedId = UUID.randomUUID().toString();
        
        dataSelection.restoreDataInfos();

        if (dataSelection.mustZip()) {
            zip = true;
        }

        logger.debug("Writing to " + preparedDir.resolve(preparedId));
        try (OutputStream stream = new BufferedOutputStream(Files.newOutputStream(preparedDir.resolve(preparedId)))) {
            Prepared.pack(  stream, 
                            zip,
                            compress, 
                            dataSelection.getDsInfo(), 
                            dataSelection.getDfInfo(), 
                            dataSelection.getEmptyDatasets(), 
                            dataSelection.getFileLength().isEmpty() ? 0 : dataSelection.getFileLength().getAsLong()
            );
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        logger.debug("preparedId is " + preparedId);

        return new ValueContainer(preparedId);
    }

    @Override
    public String getCustomRequestParametersLogString() { 
        return "zip='" + this.zip + "'' compress='" + compress + "'";
    }

    @Override
    public void addCustomParametersToTransmitterJSON(JsonGenerator gen) {
        gen.write(RequestIdNames.preparedId, preparedId);
    }

    @Override
    public CallType getCallType() {
        return CallType.PREPARE;
    }
}